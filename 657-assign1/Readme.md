This assignment was done in a team of two by Sai Sandeep Varma Mudundi(G01352322)and Rajeev Priyatam Panchadula(G01333080). 

This assignment consists of using a PySpark program to calculate word count, the average and standard deviation of a word in a window of 4 years, the frequency of co-occurrence of words, and the lift between two words. The data being used is the text transcriptions of the State of the Union Addresses given by Presidents to Congress from 1790 to the present year. The data consists of files named yyyymmdd.txt, where yyyy is the year, mm is the month, and dd is the day on which the address was given.

Our submission consists of a report, a readme file, a code folder, and an output folder. 
Run the code file in a clustered computing environment with pyspark installed in it. 
The output folder has four folders and one file. 
The output of part 1 can be found in the folders avg_std_4_yr.txt and Words_avg_2_STD_exceeding.
1.The folder avg_std_4_yr.txt contains the data's average and standard deviation.
2.Result of the words that appear in the year following the window with a frequency that exceeds the average plus two standard deviations contained in the Words_avg_2_STD_exceeding folder
3.The intermediate results of part-1 are submitted in a legible manner in the file Avg_std_output. 
The output of part2 can be found in the folders lift_3_bigger_words and word_occr_exceed_10. 
1. The word pairs whose associated lift is greater than 3 are contained in the lift_3_bigger_words folder.
2. The folder word_occr_exceed_10 contains pairs of words with a co-occurance frequency greater than 10. 
3. The text file Frequency_20 has 20 frequent words, not necessarily the top 20. 
IN EVERY FOLDER THE OUTPUT IS CONATAINED IN THE FILE WHOSE FILENAME STARTSWITHS WITH PART.

To run on persues update the file paths and run the script. reference :https://cs.gmu.edu/~dbarbara/CS657/hadoopinst.html

To output a result as file, update the correct path and uncomment the lines where you see write actions
