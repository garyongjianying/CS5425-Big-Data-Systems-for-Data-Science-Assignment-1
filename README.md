# CS5425-Big-Data-Systems-for-Data-Science-Assignment-1
CS5425 Assignment 1 - Map Reduce for TopkCommonWords in Java, given 2 text files, count the number of words that are common between the 2


Problem definition 
Given TWO textual files, for each common word between the two files, find the smaller number of times that it appears between the two files. Output the top 20 common words with highest such frequency (For words with the same frequency, there’s no special requirement for the output order).
Example: if the word “John” appears 5 times in the 1st file and 3 times in the 2nd file, the smaller number of times is 3
Requirements
Split the input text with “(space)\t\n\r\f”. Any other tokens like “,.:`” will be regarded as a part of the words
Remove stop-words as given in Stopwords.txt, such as “a”, “the”, “that”, “of”, … (case sensitive) 
Sort the common words in descending order of the smaller number of occurrences in the two files.
In general, words with different case or different non-whitespace punctuation are considered different words
![image](https://user-images.githubusercontent.com/63240580/236692589-46a43a13-5136-45c8-8842-5912bc09229c.png)
