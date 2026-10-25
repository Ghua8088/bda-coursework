--q1

lines = LOAD 'text.txt' as (line:chararray);

-- tokenize

words = ForEach lines generate flatten(tokenize(lines)) as word;

-- group 
groups = group words by word;

-- count

word_counts = ForEach groups generate group as word, count(word) as count;

--output 

Dump word_counts;