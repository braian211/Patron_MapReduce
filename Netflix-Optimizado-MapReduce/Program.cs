using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Netflix_Optimizado_MapReduce
{
    class Program
    {
        static void Main(string[] args)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();

            //
            // Process cmd-line args, welcome msg:
            //
            string infile = @"c:\users\braian\desktop\ratings.txt";
            //string infile = AppDomain.CurrentDomain.BaseDirectory + @"c:\users\braian\desktop\ratings.txt";

            // 
            // Foreach record, parse and aggregate the pairs <userid, num reviews>:
            //
            sw.Restart();

            Dictionary<int, int> ReviewsByUser = new Dictionary<int, int>();

            Parallel.ForEach(File.ReadLines(infile),

                //
                // Initializer:  create task-local Dictionary:
                //
                () => { return new Dictionary<int, int>(); },


                //
                // Loop-body: work with TLS which represents a local Dictionary,
                // mapping our results into this local dictionary:
                //
                (line, loopControl, localD) =>
                {
                    //
                    // movie id, user id, rating (1..5), date (YYYY-MM-DD)
                    //
                    int userid = parse(line);

                    if (!localD.ContainsKey(userid))  // first review:
                        localD.Add(userid, 1);
                    else  // another review by same user:
                        localD[userid]++;

                    return localD;  // return out so it can be passed back in later:
                },

                //
                // Finalizer: reduce individual local dictionaries into global dictionary:
                //
                (localD) =>
                {
                    lock (ReviewsByUser)
                    {
                        //
                        // merge into global data structure:
                        //
                        foreach (int userid in localD.Keys)
                        {
                            int numreviews = localD[userid];

                            if (!ReviewsByUser.ContainsKey(userid))  // first review:
                                ReviewsByUser.Add(userid, numreviews);
                            else  // another review by same user:
                                ReviewsByUser[userid] += numreviews;
                        }
                    }
                }

            );

            //
            // Sort pairs by num reviews, descending order, and take top 10:
            //
            var top10 = ReviewsByUser.OrderByDescending(x => x.Value).Take(10);

            long timems = sw.ElapsedMilliseconds;

            //
            // Write out the results:
            //
            Console.WriteLine();
            Console.WriteLine("** Top 10 users reviewing movies:");

            foreach (var user in top10)
                Console.WriteLine("{0}: {1}", user.Key, user.Value);

            // 
            // Done:
            //
            double time = timems / 1000.0;  // convert milliseconds to secs

            Console.WriteLine();
            Console.WriteLine("** Done! Time: {0:0.000} secs", time);
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();

            Console.Write("Press a key to exit...");
            Console.ReadKey();
        }


        /// <summary>
        /// Parses one line of the netflix data file, and returns the userid who reviewed the movie.
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        private static int parse(string line)
        {
            char[] separators = { ',' };

            string[] tokens = line.Split(separators);

            int movieid = Convert.ToInt32(tokens[0]);
            int userid = Convert.ToInt32(tokens[1]);
            int rating = Convert.ToInt32(tokens[2]);
            DateTime date = Convert.ToDateTime(tokens[3]);

            return userid;
        }

    }//class
}//namespace

        
    

