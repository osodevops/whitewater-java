package dev.whitewater.model;

import java.time.Duration;
import java.util.Objects;

/**
 * A length of time expressed in human units — {@link #h(long) hours},
 * {@link #m(long) minutes}, {@link #s(long) seconds},
 * {@link #ms(long) milliseconds}, {@link #us(long) microseconds},
 * {@link #d(long) days}, or {@link #ns(long) nanoseconds}. SDK calls
 * that take a duration ({@code PipeOperation.window}, subscription
 * delivery delay, and friends) accept a {@code Time}; the SDK extracts
 * nanoseconds internally for the wire — callers never spell raw
 * nanosecond counts.
 *
 * <p>{@code Time} is an immutable value: {@link #equals(Object)} and
 * {@link #hashCode()} compare by duration, so {@code Time.m(5)} equals
 * {@code Time.s(300)}.
 */
public final class Time implements Comparable<Time> {

    private static final long NS_PER_US = 1_000L;
    private static final long NS_PER_MS = 1_000_000L;
    private static final long NS_PER_S = 1_000_000_000L;
    private static final long NS_PER_M = 60L * NS_PER_S;
    private static final long NS_PER_H = 60L * NS_PER_M;
    private static final long NS_PER_D = 24L * NS_PER_H;

    private final long nanos;

    private Time(long nanos) {
        this.nanos = nanos;
    }

    /** Nanoseconds. */
    public static Time ns(long nanoseconds) {
        return new Time(nanoseconds);
    }

    /** Microseconds. */
    public static Time us(long microseconds) {
        return of(microseconds, NS_PER_US, "microseconds");
    }

    /** Milliseconds. */
    public static Time ms(long milliseconds) {
        return of(milliseconds, NS_PER_MS, "milliseconds");
    }

    /** Seconds. */
    public static Time s(long seconds) {
        return of(seconds, NS_PER_S, "seconds");
    }

    /** Minutes. */
    public static Time m(long minutes) {
        return of(minutes, NS_PER_M, "minutes");
    }

    /** Hours. */
    public static Time h(long hours) {
        return of(hours, NS_PER_H, "hours");
    }

    /** Days. */
    public static Time d(long days) {
        return of(days, NS_PER_D, "days");
    }

    /** The {@link Duration} equivalent — {@code Time.of(Duration.ofMinutes(5))}. */
    public static Time of(Duration duration) {
        Objects.requireNonNull(duration, "duration is required");
        try {
            return new Time(duration.toNanos());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "duration overflows nanoseconds", overflow);
        }
    }

    private static Time of(long amount, long unitNs, String unit) {
        try {
            return new Time(Math.multiplyExact(amount, unitNs));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    amount + " " + unit + " overflows nanoseconds", overflow);
        }
    }

    /**
     * The duration in nanoseconds — the internal/wire encoding only;
     * callers express durations with the unit factories above.
     */
    public long nanos() {
        return nanos;
    }

    /** This duration as a {@link Duration}. */
    public Duration duration() {
        return Duration.ofNanos(nanos);
    }

    /** {@code true} when this duration is greater than zero. */
    public boolean isPositive() {
        return nanos > 0;
    }

    @Override
    public int compareTo(Time other) {
        return Long.compare(nanos, other.nanos);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Time time && nanos == time.nanos;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(nanos);
    }

    /** Compact rendering in the largest exact unit — {@code "1h"}, {@code "5m"}, {@code "30s"}. */
    @Override
    public String toString() {
        if (nanos == 0) {
            return "0s";
        }
        if (nanos % NS_PER_D == 0) {
            return (nanos / NS_PER_D) + "d";
        }
        if (nanos % NS_PER_H == 0) {
            return (nanos / NS_PER_H) + "h";
        }
        if (nanos % NS_PER_M == 0) {
            return (nanos / NS_PER_M) + "m";
        }
        if (nanos % NS_PER_S == 0) {
            return (nanos / NS_PER_S) + "s";
        }
        if (nanos % NS_PER_MS == 0) {
            return (nanos / NS_PER_MS) + "ms";
        }
        if (nanos % NS_PER_US == 0) {
            return (nanos / NS_PER_US) + "us";
        }
        return nanos + "ns";
    }
}
