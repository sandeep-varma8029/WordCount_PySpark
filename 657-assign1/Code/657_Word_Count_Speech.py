# -*- coding: utf-8 -*-
# Pyspark installation if not already installed

# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
# !pip install pyspark

# rm -r files/

"""Get files from URL to local storage"""

url = "http://stateoftheunion.onetwothree.net/texts/index.html"
headers = {'User-Agent': "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7"}
import urllib.request
try: response=urllib.request.urlopen(urllib.request.Request(url, None, headers)).read()
except urllib.error.URLError as e: print(e)

import re, os
name=re.findall(r'"(.*?)\"',''.join(re.findall(r'<li>(.*?)</li>',str(response))[5:]))
os.makedirs('files')
for i in name:
    try: fetch=urllib.request.urlopen(urllib.request.Request(('http://stateoftheunion.onetwothree.net/texts/'+i),None,headers)).read()
    except urllib.error.URLError as e: print(e)
    with open(os.path.join("./files",i.split('.')[0]), 'w') as file: file.write(str(fetch))

"""Start spark

"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
#replace path here and unccoment to print the output file , for persues : hdfs://perseus.cecnet.gmu.edu:9999/user/<gmuid>/*
rdd = sc.wholeTextFiles('./files/*')

#Preprocessing
#removing the content before the paragrapgh
remove_tags=rdd.map(lambda itr: (itr[0].split('/')[-1],itr[1].split("</h3>")[1]))
#removing content below the paragraph
remove_footer=remove_tags.map(lambda x: (x[0],x[1].split('<div class="textNav">')[0].lower()))
#removing html commands in brackets
remove_htags=remove_footer.map(lambda x:( x[0],re.sub(r"<[^<]+?>", " ", x[1])))
#removing new line  and tabs
remove_newline=remove_htags.map(lambda x:(x[0],x[1].replace('\\n',' ')))
remove_tabs=remove_newline.map(lambda x:(x[0],x[1].replace('\\t',' ')))
#removing extra white space
remove_exspace=remove_tabs.map(lambda x:( x[0],re.sub(r'\s+', ' ', x[1])))
#removing alpha numeric
remove_alphanum=remove_exspace.map(lambda x:( x[0],re.sub(r"[^a-zA-Z .]", " ", x[1])))
# removing digits
remove_digits=remove_alphanum.map(lambda x:( x[0],re.sub(r"(\\d+)", " ", x[1])))
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
#creating a list of stop words to be removed
stop_words=list(map(lambda x: ' '+x+' ',stopwords.words('english')))
stop_words_new=stop_words+['.']
#using two function to prepare data for part 1 and part 2
def remv_stopwords(x):
    for i in stop_words_new: x=(x[0],x[1].replace(i,' '))
    return (x[0],' '.join(x[1].split()))
def remv_stopwords_2(x):
    for i in stop_words: x=(x[0],x[1].replace(i,' '))
    return (x[0],' '.join(x[1].split()))
prc_pt1=remove_digits.map(remv_stopwords)
split_sentence=remove_digits.map(remv_stopwords_2)
#use the below for part 2 as it has '.' punctuation. This helps in split of sentences
split_even=split_sentence.map(lambda x: (x[0],x[1].replace('.',' . ')))

"""**Part 1**"""

import math
years=prc_pt1.map(lambda x: int(x[0][:4]))
#to get the range from 2009
year_val=math.ceil((years.max()-2009)/4)
#getting the total years
total_years=years.max()-years.min()
#converting the speech into a word list
tokenize=prc_pt1.flatMap(lambda x: x[1].split())
#intially all count of a word occurence is set to 1
tokenize1=tokenize.map(lambda x: (x,1))
#using below code to reduce by the word key to get the occurence of a particular word
tokenize2=tokenize1.reduceByKey(lambda x,y: x+y)\
#In this step we divide the frequency by total number of years to get the total average
total_avg=tokenize2.map(lambda x: (x[0],x[1]/total_years))


#total_avg.saveAsTextFile('total_avg')
#adding date to word list
wplusy=prc_pt1.flatMap(lambda x: [((x[0],y),1) for y in x[1].split(' ')])
#count of occurance of a particular word in each year
get_frequency=wplusy.reduceByKey(lambda x,y: x+y)
#gives us the correct yyyy format of the date
cmb_dt_wrd=get_frequency.map(lambda x: ((x[0][0][:4],x[0][1]),x[1]))
#starting to take data from 2009
words_09=cmb_dt_wrd.filter(lambda x: int(x[0][0])>=2009)
#Forming the range of the window i.e 2009 to 2012, 2013 to 2016
range1_split=words_09.map(lambda x: ((str(int(x[0][0])-(((int(x[0][0])-1)%4))),x[0][1]),x[1]))
word_range=range1_split.reduceByKey(lambda x,y: x+y)
y4_range=word_range.map(lambda x: (x[0][1],(x[0][0]+' to '+str(int(x[0][0])+3),x[1])))
#Finding the average number of times a word appears in the 4 year window
word_avg=y4_range.map(lambda x: (x[0],(x[1][0],x[1][1]/4)))
final_avg=word_avg.groupByKey().mapValues(list)



compute_std1=y4_range.map(lambda x: (x[0],x[1][1]))
compute_std2=compute_std1.groupByKey().mapValues(list)
#calculating the standard deviation using the one-liner (sum((x-avg)**2 for x in lst) / len(lst))**0.5
final_std=compute_std2.map(lambda x: (x[0],(sum([(y-(sum(x[1])/year_val))**2 for y in x[1]])/year_val)**0.5))
stdpavg=final_avg.join(final_std)

# Words that appear in year following window with frequency exceeding 2 std-devs
word_std=stdpavg.map(lambda x: (x[0],x[1][1]))
#getting the next year number
nxtyr=word_avg.map(lambda x: (x[0],(int(x[1][0].split(' to ')[1])+1,x[1][1])))

#creating the avg plus 2 standard deviation threshold limit
thres1=nxtyr.join(word_std)
thres2=thres1.map(lambda x: ((x[0],str(x[1][0][0])),x[1][0][1]+2*x[1][1]))


yrmapp=words_09.map(lambda x: ((x[0][1],x[0][0]),x[1]))


cmb_thres=thres2.join(yrmapp)
#using condition to check if frequency is greater than average plus two standard deviations for the following year
fltr_freq=cmb_thres.map(lambda x: (True if x[1][0]<x[1][1] else False,x[0]))
fltr_out=fltr_freq.filter(lambda x: x[0])
std_exd_2_wrds=fltr_out.map(lambda x: x[1]).map(lambda x: x[0]).distinct()
#replace path here and unccoment to print the output file
# std_exd_2_wrds.coalesce(1).saveAsTextFile('Words_avg_2_STD_exceeding')
#replace path here and unccoment to print the output file
# stdpavg.coalesce(1).saveAsTextFile('avg_std_4_yr.txt')
#replace path here and unccoment to print the output file
# f = open('/content/Avg_std_output','w')
# for x in stdpavg.collect():
#   for i in range(len(x[1][0])):
#    print(f"The word {x[0]} in the window of: {x[1][0][i][0]} appears with an average of {x[1][0][i][1]} and standard deviation(during {x[1][0][i][0]} window ) is {x[1][1]}",file=f)

"""**Part 2**"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, trim, col
import pyspark.sql.functions as func
from pyspark.sql.functions import size 
spark = SparkSession.builder.appName('WordCount_part_2').getOrCreate()
#Taking the preprocessed text to split by line occucrences 
#converting the rdd into a dataframe
speech_df=split_even.toDF(['date','speech'])

scentence_split=speech_df.withColumn("speech",explode(split(col("speech"),' . ')))
rmv_date=scentence_split.drop('date')
drp_dpl=rmv_date.dropDuplicates()
processed_sentences=drp_dpl.filter(drp_dpl.speech!='').filter(drp_dpl.speech!=' ')

processed_sentences_conv=processed_sentences.withColumn("wordlist", processed_sentences["speech"])
sentence_word_list=processed_sentences_conv.withColumn('wordlist',explode(split(trim(col("wordlist")),' ')))
final_words=sentence_word_list.filter(sentence_word_list.wordlist!='')
#To take combinations of a pair of words, the speech dataframe is split into two duplicate dataframes. So when joined using the below condition creates different combinations
speech_df_A=final_words.alias('speech_df_A').withColumnRenamed('wordlist','wordsA')
speech_df_B=final_words.alias('speech_df_B').withColumnRenamed('wordlist','wordsB').withColumnRenamed('speech','speech_dup')

combine=speech_df_A.join(speech_df_B,[speech_df_A.speech==speech_df_B.speech_dup,speech_df_A.wordsA<speech_df_B.wordsB],'inner').drop('speech_dup')



all_combinations=combine.groupBy(['speech','wordsA','wordsB']).count()

#Consider only pairs of words that appear together more than 10 times
combinations=all_combinations.where(func.col('count')>10).drop('speech')


#Saving into a file
#replace path here and unccoment to print the output file
# combinations.coalesce(1).write.csv("word_occr_exceed_10")
# number of sentences found in ALL the speeches
N_sentences=processed_sentences.count()


unq_words=processed_sentences.withColumn("uniquewords",processed_sentences["speech"]).withColumn('uniquewords',explode(split(trim(col('uniquewords')),' ')))
grp_with_words=unq_words.groupBy(['speech','uniquewords']).count()

words_repeats=grp_with_words.select('uniquewords').groupBy('uniquewords').count()

#P(A) = probability of word A = frequency(A)/N_sentences
word_probability=words_repeats.withColumn('prob',col('count')/N_sentences).drop('count')

wordA_prob=all_combinations.join(word_probability,[all_combinations.wordsA==word_probability.uniquewords],'inner').drop('uniquewords').withColumnRenamed('prob','prob(word_A)')
#P(B) = probability of word B = frequency(B)/N_sentences
wordB_prob=wordA_prob.join(word_probability,[wordA_prob.wordsB==word_probability.uniquewords],'inner').drop('uniquewords').withColumnRenamed('prob','prob(word_B)')
comd=all_combinations.select('wordsA','wordsB').groupBy(['wordsA','wordsB']).count()
#P(AB)= probability of the pair AB = frequency(AB)/N_sentences
comb_prob=comd.withColumn('prob(AB)',col('count')/N_sentences).drop('count').withColumnRenamed('wordsA','w_A').withColumnRenamed('wordsB','w_B')

final=wordB_prob.join(comb_prob,[wordB_prob.wordsA==comb_prob.w_A,wordB_prob.wordsB==comb_prob.w_B],'inner').drop('speech','count','w_A','w_B').dropDuplicates()
final=final.withColumn('P(A/B)',col('prob(AB)')/col('prob(word_B)')).withColumn('P(B/A)',col('prob(AB)')/col('prob(word_A)'))
new_f=final.where(final.wordsA!='.').where(final.wordsB!='.').dropDuplicates()
#Lift(AB) = P(AB)/(P(A)*P(B))
lift=new_f.withColumn('Lift',col('prob(AB)')/(col('prob(word_A)')*col('prob(word_B)')))
lift_filter=lift.filter(col('Lift')>3).dropDuplicates()
#replace path here and unccoment to print the output file
# lift_filter.coalesce(1).write.csv("lift_3_bigger_words")