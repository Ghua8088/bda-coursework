--q1
-- 1. Load the input text file (line by line)
lines = LOAD 'input.txt' USING PigStorage('\n') AS (line:chararray);

-- 2. Tokenize: Convert each line into individual words
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- 3. Group identical words together
grouped_words = GROUP words BY word;

-- 4. Count occurrences per word
word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(words) AS count;

-- 5. Order results descending
sorted_counts = ORDER word_counts BY count DESC;

-- 6. Display output
DUMP sorted_counts;

--q2
-- 1. Load sentiment dictionary (word, score: 1 or -1)
dictionary = LOAD 'dictionary.tsv' USING PigStorage('\t') 
             AS (dict_word:chararray, score:int);

-- 2. Load tweets dataset
tweets = LOAD 'united_airlines_tweets.tsv' USING PigStorage('\t') 
         AS (tweet_id:chararray, permalink:chararray, date_posted:chararray, tweet_text:chararray, username:chararray);

-- 3. Extract words from tweets while keeping tweet_id
tweet_words = FOREACH tweets GENERATE tweet_id, tweet_text, FLATTEN(TOKENIZE(LOWER(tweet_text))) AS word;

-- 4. Join tweet words with sentiment dictionary
joined_words = JOIN tweet_words BY word, dictionary BY dict_word;

-- 5. Group by tweet ID and tweet text
grouped_tweets = GROUP joined_words BY (tweet_words::tweet_id, tweet_words::tweet_text);

-- 6. Compute total sentiment score and classify as POSITIVE or NEGATIVE
sentiment_scores = FOREACH grouped_tweets GENERATE 
    group.tweet_id AS tweet_id,
    group.tweet_text AS tweet_text,
    SUM(joined_words.score) AS total_score,
    (SUM(joined_words.score) > 0 ? 'POSITIVE' : 'NEGATIVE') AS sentiment;

-- 7. Display results
DUMP sentiment_scores;
--q3
-- Load crime dataset
crime_data = LOAD 'crime_data.csv' USING PigStorage(',') 
             AS (case_number:chararray, primary_category:chararray, description:chararray, location:chararray, arrest_status:chararray, theft_cost:float);

-- Task 1: Filter where theft costs more than $500[cite: 1]
theft_over_500 = FILTER crime_data BY (LOWER(primary_category) == 'theft') AND (theft_cost > 500.00);[cite: 1]
DUMP theft_over_500;[cite: 1]

-- Task 2: Project the data with crime category and location[cite: 1]
projected_data = FOREACH crime_data GENERATE primary_category, location;[cite: 1]
DUMP projected_data;[cite: 1]

-- Task 3 & 4: Determine total cases for each major category, ordered from highest to lowest[cite: 1]
grouped_categories = GROUP crime_data BY primary_category;[cite: 1]
category_counts    = FOREACH grouped_categories GENERATE group AS primary_category, COUNT(crime_data) AS total_cases;[cite: 1]
sorted_categories  = ORDER category_counts BY total_cases DESC;[cite: 1]
DUMP sorted_categories;[cite: 1]