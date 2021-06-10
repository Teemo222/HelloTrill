using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;

namespace HelloTrill
{
    class Program
    {
        static void Main(string[] args)
        {
            /**
             * Generating synthetic data
             */
            var SIZE = 1000; // Size of the data set
            var listA = new List<int>(); // A list for storing the data points
            var listB = new List<int>(); // Another list for storing the data points
            var listC = new List<int>();
            for (int i = 0; i < SIZE; i++)
            {
                listA.Add(i); // Populate listA with dummy data
                listB.Add(i); // Populate listB with dummy data
                if (i > 50 && i < 100 || i > 200 && i < 400) {}
                else { listC.Add(i); }
            }

            /**
             * Creating lists created above to Trill streams
             */
            var streamA = listA // Creating first stream from listA 
                    .ToObservable() // Convert the data list to an Observable first 
                    .ToTemporalStreamable(e => e, e => e + 1) // Then convert to Trill temporal stream;
                ; // nth event in the stream has an integer payload 'n'
            // and an interval of [n, n+1)

            var streamB = listB // Creating streamB (not using yet) similar to streamA.
                    .ToObservable()
                    .ToTemporalStreamable(e => e, e => e + 100)
                ;
            
            var streamC = listC // Creating streamC, which has gaps
                    .ToObservable()
                    .ToTemporalStreamable(e => e, e => e + 1)
                ;
            
            // ========== Exercise 1.1 =============
            // var level1_1 = streamA
            //     .Select(e => e + 1);

            // ========== Exercise 1.2 =============
            // var level1_2 = streamA
            //     .Select(e => 3 * e);
            
            // ========== Exercise 1.3 =============
            // var level1_3 = streamA
            //     .Where(e => e % 2 == 1);

            // ========== Exercise 1.4 =============
            // var level1_4 = streamA
            //     .Select(e => 3 * (e + 1)) 
            //     .Where(e => e % 2 == 1);

            // ========== Exercise 2.1 =============
            // var level2_1_window_size = 10;
            // var level2_1 = streamA
            //     .ShiftEventLifetime(1)
            //     .TumblingWindowLifetime(level2_1_window_size)
            //     .ShiftEventLifetime(-level2_1_window_size)
            //     .Sum(e => e);
            
            // ========== Exercise 2.2 =============
            // var level2_2 = streamA
            //     .Join(streamB, (A, B) => A + B);
            
            // ========== Exercise 2.3 =============
            // var level2_3 = streamA.
            //     Join(streamB, (A, B) => new {A, B});

            // ========== Exercise 2.4 =============
            // var level2_4_window_size = 10;
            // var level2_4 = streamB
            //     .ShiftEventLifetime(1)
            //     .TumblingWindowLifetime(level2_4_window_size)
            //     .ShiftEventLifetime(-level2_4_window_size)
            //     .Sum(e => e);

            // ========== Exercise 2.5 =============
            // var level2_5_window_size = 10;
            // var level2_5 = streamA.Multicast(s => s
            //     .ShiftEventLifetime(1)
            //     .TumblingWindowLifetime(level2_5_window_size)
            //     .ShiftEventLifetime(-level2_5_window_size)
            //     .Sum(e => e)
            //     .Join(s, (l, r) => new {l, r})
            // );

            // ========== Exercise 2.6 =============
            // var level2_6_window_size = 10;
            // var level2_6 = streamA
            //     .Multicast(a => a
            //         .ShiftEventLifetime(1)
            //         .TumblingWindowLifetime(level2_6_window_size)
            //         .ShiftEventLifetime(-level2_6_window_size)
            //         .Multicast(s => s.Average(e => e).Join(s
            //                 .Aggregate(w => w.StandardDeviation(e => e)),
            //             (avg, sd) => new {Avg = avg, Sd = sd}))
            //         .Join(a, (l, r) => (r - l.Avg) / l.Sd));
            
            // ========== Exercise 2 dot 5 =============
            // var level2dot5_window_size = 10
            // var level2dot5_stride_length = 5

            // var level2dot5 = streamA
            //     .ShiftEventLifetime(1)
            //     .HoppingWindowLifetime(level2dot5_window_size, level2dot5_stride_length)
            //     .ShiftEventLifetime(-level2dot5_stride_length)
            //     .Average(e => e)
            //     .AlterEventLifetime(
            //         start => (start - level2dot5_stride_length) >= 0 ? (start - level2dot5_stride_length) : 0,
            //         start => (start - level2dot5_stride_length) >= 0 ? level2dot5_window_size : level2dot5_stride_length); 
            
            // var fixedInterval = new[] { StreamEvent.CreateInterval(0, 1000, 10) }
            //     .ToObservable().ToStreamable();

            // ========== Exercise 3.1 =============
            // var level3_1 = streamC
            //     .AlterEventDuration(StreamEvent.InfinitySyncTime)
            //     .Multicast(s => s
            //         .ClipEventDuration(s))
            //     .Join(fixedInterval, (l, r) => l);

            // ========== Exercise 3.2 =============
            // var level3_2 = level3_1
            //     .Chop(0, 1);
            
            // ========== Exercise 3.3 =============
            // var level3_3 = level3_2
            //     .Select((origStartTime, e) => origStartTime);

            long tolerance_gap = 100;
            
            // ========== Exercise 3.4 =============
            // var level3_4 = streamC
            //     .AlterEventLifetime(origStartTime => origStartTime,
            //         (start, end) => end - start + tolerance_gap)
            //     .Multicast(s => s.ClipEventDuration(s))
            //     .AlterEventLifetime(startTime => startTime,
            //         (startTime, endTime) => (endTime - startTime) > tolerance_gap ? 1 : (endTime - startTime))
            //     .Chop(0, 1);

            // ========== Exercise 4.1 ============= 
            int W = 300;
            var rollingMean = streamC
                .HoppingWindowLifetime(W, 1)
                .Average(e => e)
                .Chop(0, 1);

            var gaps_filled = streamC
                .AlterEventLifetime(origStartTime => origStartTime,
                    (start, end) => end - start + tolerance_gap)
                .Multicast(s => s.ClipEventDuration(s))
                .AlterEventLifetime(startTime => startTime,
                    (startTime, endTime) => (endTime - startTime) > tolerance_gap ? 1 : (endTime - startTime))
                .Chop(0, 1);

            var gaps_with_rolling_means = gaps_filled
                .WhereNotExists(streamC)
                .Join(rollingMean, (l, r) => r);

            var level4_1 = gaps_with_rolling_means
                .Union(streamC.Select(e => (double) e));
            
            // ========== Exercise 4.2 =============
            // Suppose we want to transform a stream with interval size 3 to interval size 2.
            var original_interval_size = 3;
            var new_interval_size = 2;

            // Create a stream with interval size `original_interval_size`
            var listD = new List<int>();
            for (int i = 0; i < 100; i += original_interval_size)
            {
                listD.Add(i);
            }

            var streamD = listD
                .ToObservable()
                .ToTemporalStreamable(e => e, e => e + original_interval_size)
                .Select(e => (e + 2) * 7 % 4); // This is to randomly set the stream payload

            var upsampled = streamD
                .Chop(0, new_interval_size)
                .Select((origStartTime, e) => origStartTime)
                .Where(e => e % new_interval_size == 0)
                .AlterEventDuration(1);

            var temp = streamD
                .Chop(0, 1)
                .Join(streamD, (l, r) => r);

            var temp2 = streamD
                .Chop(0, 1)
                .ShiftEventLifetime(original_interval_size)
                .Join(streamD, (l, r) => r)
                .ShiftEventLifetime(-original_interval_size);

            var temp3 = temp
                .Join(temp2, (l, r) => new {left = (double) l, right = (double) r});

            var level4_2 = upsampled
                .Join(temp3, (l, r) =>
                    r.left + (l % original_interval_size) * ((r.right - r.left) / original_interval_size))
                .AlterEventDuration(new_interval_size);

            // ========== Print any resulting stream =============
            level4_2
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e =>
                {
                    Console.WriteLine(e);
                })        // Print the events to the console
                ;
        }
    }
}