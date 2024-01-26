use v6.d;

my $original-scheduler = INIT $*SCHEDULER;

class X::Test::Scheduler::BackInTime is Exception {
    method message() {
        "Test scheduler can not go backwards in time";
    }
}

class Test::Scheduler does Scheduler {
    my class FutureEvent {
        has &.schedulee is required;
        has $.virtual-time is required;
        has $.reschedule-after;
        has $.cancellation;
    }

    has $!wrapped-scheduler;
    has $.virtual-time = now;
    has $!virtual-target = $!virtual-time;
    has @!future;
    has $!lock = Lock.new;

    submethod BUILD(
        :wrap($!wrapped-scheduler) = $original-scheduler,
        :$!virtual-time = now
    ) { }

    method cue(&code, :$at, :$in, :$every, :$times = 1, :&stop is copy, :&catch ) {
        die "Cannot specify :at and :in at the same time"
          if $at.defined and $in.defined;
        die "Cannot specify :every, :times and :stop at the same time"
          if $every.defined and $times > 1 and &stop;
        my $delay = $at ?? $at - $!virtual-time !! $in // 0;

        # need repeating
        if $every {
            # generate a stopper if needed
            if $times > 1 {
                my $todo = $times;
                my $times-lock = Lock.new;
                &stop = { $times-lock.protect: { $todo ?? !$todo-- !! True } }
            }

            # we have a stopper
            if &stop {
                my $cancellation = Cancellation.new;
                $!lock.protect: {
                    push @!future, FutureEvent.new(
                        schedulee => &catch
                            ?? -> {
                                my $*SCHEDULER = self;
                                stop()
                                    ?? $cancellation.cancel
                                    !! code();
                                CATCH { default { catch($_) } };
                            }
                            !! -> {
                                my $*SCHEDULER = self;
                                stop()
                                    ?? $cancellation.cancel
                                    !! code();
                            },
                        virtual-time => $!virtual-time + $delay,
                        reschedule-after => $every,
                        cancellation => $cancellation
                    );
                }
                return $cancellation;
            }
            # no stopper
            else {
                my $cancellation = Cancellation.new;
                $!lock.protect: {
                    push @!future, FutureEvent.new(
                        schedulee => &catch
                            ?? -> {
                                my $*SCHEDULER = self;
                                code();
                                CATCH { default { catch($_) } }
                            }
                            !! -> {
                                my $*SCHEDULER = self;
                                code()
                            },
                        virtual-time => $!virtual-time + $delay,
                        reschedule-after => $every,
                        cancellation => $cancellation
                    );
                }
                return $cancellation;
            }
        }

        # only after waiting a bit or more than once
        elsif $delay or $times > 1 {
            my &schedulee := &catch
                ?? -> {
                    my $*SCHEDULER = self;
                    code();
                    CATCH { default { catch($_) } }
                }
                !! -> {
                    my $*SCHEDULER = self;
                    code();
                };
            my $cancellation = Cancellation.new;
            my $virtual-time = $!virtual-time + $delay;
            $!lock.protect: {
                for 1..$times {
                    @!future.push: FutureEvent.new(:&schedulee, :$virtual-time, :$cancellation);
                }
            }
            return $cancellation;
        }

        else {
            with @*TEST-SCHEDULER-NESTED -> @nested {
                # Already running code under the test scheduler. Delegate to
                # the wrapped scheduler.
                my $p = Promise.new(scheduler => $!wrapped-scheduler);
                $!lock.protect: { @nested.push($p) };
                $!wrapped-scheduler.cue: &catch
                    ?? (
                        {
                            my $*SCHEDULER = self;
                            code();
                            CATCH { default { catch($_) } } 
                            LEAVE $p.keep(True);
                       })
                   !! (
                        {
                            my $*SCHEDULER = self;
                            code();
                            LEAVE $p.keep(True);
                       });
            }
            else {
                # Schedule the code at the current virtual time
                my &schedulee = &catch
                    ?? (
                        {
                            my $*SCHEDULER = self;
                            code();
                            CATCH { default { catch($_) } }
                        })
                    !! (
                        {
                            my $*SCHEDULER = self;
                            code()
                        });
                $!lock.protect: {
                    @!future.push: FutureEvent.new(:&schedulee, :$!virtual-time);
                }
            }
            return Nil;
        }
    }

    method advance(--> Nil) {
        self!run-due();
    }

    method advance-by($seconds --> Nil) {
        die X::Test::Scheduler::BackInTime.new if $seconds < 0;
        $!virtual-target += $seconds;
        self!run-due();
        $!virtual-time = $!virtual-target;
    }

    method advance-to(Instant $new-virtual-time --> Nil) {
        die X::Test::Scheduler::BackInTime.new if $new-virtual-time < $!virtual-time;
        $!virtual-target = $new-virtual-time;
        self!run-due();
        $!virtual-time = $!virtual-target;
    }

    method !run-due($target = $!virtual-target) {
        loop {
            my (@now, @future) := $!lock.protect: {
                my (:@now, :@future) := @!future.classify: {
                    .virtual-time <= $target ?? 'now' !! 'future'
                }
                return unless @now;
                @!future = ();
                @now, @future
            }

            my @sorted = @now.sort(*.virtual-time);
            my @working;
            for @sorted.kv -> $i, $_ {
                next if .cancellation.?cancelled;

                if $!virtual-time != .virtual-time {
                    await @working;
                    @working = ();
                    if $!lock.protect: { so @!future } {
                        @future.append(@sorted[$i .. *]);
                        last;
                    }
                }
                $!virtual-time = .virtual-time;

                given .schedulee -> &to-run {
                    my $done = Promise.new(scheduler => $!wrapped-scheduler);
                    $!wrapped-scheduler.cue({
                        my @*TEST-SCHEDULER-NESTED = ();
                        to-run();
                        await @*TEST-SCHEDULER-NESTED;
                        LEAVE $done.keep(True);
                    });
                    push @working, $done;
                }
                if .reschedule-after {
                    my $next-time = .virtual-time + .reschedule-after;
                    @future.push(.clone(
                        virtual-time => $next-time
                    ));
                    if $next-time < $target {
                        @future.append(@sorted[$i + 1 .. *]);
                        last;
                    }
                }
            }
            await @working;
            $!lock.protect: { @!future.append(@future) }
        }
    }

    method uncaught_handler(|c) is raw {
        $!wrapped-scheduler.uncaught_handler(|c)
    }

    method handle_uncaught(|c) is raw {
        $!wrapped-scheduler.handle_uncaught(|c)
    }

    method loads(|c) is raw {
        $!wrapped-scheduler.loads(|c)
    }
}

=begin pod

=head1 NAME

Test::Scheduler - A Raku scheduler with virtualized time

=head1 SYNOPSIS

=begin code :lang<raku>

use Test;
use Test::Scheduler;

sub timeout($source, $timeout) {
    supply {
        whenever $source -> $value {
            state $values++;
            emit $value;

            my $last-values = $values;
            whenever Promise.in($timeout) {
                if $last-values == $values {
                    die "Timed out";
                }
            }
        }
    }
}

{
    my $*SCHEDULER = Test::Scheduler.new;
    my $test-source = supply {
        for 1, 2, 5 {
            whenever Promise.in($_) {
                emit 'badger';
            }
        }
    }
    my $timed-out = timeout($test-source, 2);
    my @received;
    my $died = False;
    $timed-out.tap:
        { @received.push($_) },
        quit => { $died = True }

    is @received, [], 'No values yet';

    $*SCHEDULER.advance-by(1);
    is @received, ['badger'], 'one value after 1s';
    nok $died, 'No timeout yet';

    $*SCHEDULER.advance-by(1);
    is @received, ['badger', 'badger'], 'Two value after 2s';
    nok $died, 'No timeout yet';

    $*SCHEDULER.advance-by(1);
    is @received, ['badger', 'badger'], 'Still two value after 3s';
    nok $died, 'Still not timed out yet';

    $*SCHEDULER.advance-by(1);
    is @received, ['badger', 'badger'], 'Still two value after 4s';
    ok $died, 'And have timed out';
}

done-testing;

=end code

=head1 DESCRIPTION

An implementation of the Raku C<Scheduler> role that uses virtualized
time.  This allows for testing of code depending on constructs like
C<Promise.in(...)> and C<Supply.interval(...)> more quickly and
reliably than would be possible if real time were used.

=head1 AUTHOR

Jonathan Worthington

=head1 COPYRIGHT AND LICENSE

Copyright 2016 - 2024 Jonathan Worthington

Copyright 2024 Raku Community

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
