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
            var SIZE = 1000;                                    // Size of the data set
            var listA = new List<int>();                        // A list for storing the data points
            var listB = new List<int>();                        // Another list for storing the data points
            for (int i = 0; i < SIZE; i++)
            {
                listA.Add(i);                                    // Populate listA with dummy data
                listB.Add(i);                                    // Populate listB with dummy data
            }
            
            /**
             * Creating lists created above to Trill streams
             */
            var streamA = listA                                 // Creating first stream from listA 
                    .ToObservable()                             // Convert the data list to an Observable first 
                    .ToTemporalStreamable(e => e, e => e + 1)   // Then convert to Trill temporal stream;
                ;                                               // nth event in the stream has an integer payload 'n'
                                                                // and an interval of [n, n+1)
            
            var streamB = listB                                 // Creating streamB (not using yet) similar to streamA.
                    .ToObservable()
                    .ToTemporalStreamable(e => e, e => e + 100)
                ;
            
            /**
             * Define transformations on the stream(s) 
             */
            var level1_result = streamA
                    .Select(e => 3 * (e + 1))                         // Set transformations on the stream.
                    .Where(e => e % 2 == 1)

                ;                                               // In this case, Adding 1 to each payload using Select
            
            /**
             * Print out the result
             
            level1_result
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            */

            /*
            var level2_result = streamA
                .TumblingWindowLifetime(10)
                .Sum(e => e);
            level2_result
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            */

            /*
            var joinA_B = streamA.Join(streamB, e => e, e => e, (l, r) => l + r);
            joinA_B
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            */
            
            
            
            var joinA_B_pair = streamA.Join(streamB, e => e, e => e, (l, r) => new {l, r});
            joinA_B_pair
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            
        }
    }
}