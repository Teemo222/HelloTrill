using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace Test
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
            for (int i = 0; i < SIZE; i++)
            {
                listA.Add(i); // Populate listA with dummy data
                listB.Add(i); // Populate listB with dummy data
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

            
            
            // 1.1
            var level1_1 = streamA
                .Select(e => e + 1);

            var level1_2 = streamA
                .Select(e => 3 * e);

            var level1_3 = streamA
                .Select(e => 3 * (e + 1)) 
                .Where(e => e % 2 == 1);

            var level2_1 = streamA.ShiftEventLifetime(1)
                .TumblingWindowLifetime(10)
                .ShiftEventLifetime(-10)
                .Sum(e => e);

            
            streamB
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            
            /*
            
            var level2_2 = streamA.
                Join(streamB, (l, r) => l + r);
            
            level2_2
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            
            */


            /*
            var joinA_B_pair = streamA.Join(streamB, e => e, e => e, (l, r) => new {l, r});
            joinA_B_pair
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            */

            /*
             var window_sum_B = streamB
                .TumblingWindowLifetime(10)
                .Sum(e => e)
                .Join(streamA, (l, r) => new{ l, r });
            streamA
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            */
            // streamA.Multicast(s => s.TumblingWindowLifetime(10).Sum(e => e));

        }
    }
}