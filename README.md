A few simple map reduce jobs for demonstration and learning purposes.

# Wordcount
Wordcount is a job to count the number of words in multiple files

ant tasks:

* wordcount-clean
* wordcount-upload-input
* wordcount-compile
* wordcount-run
* wordcount-output

To run all at once:
wordcount

# Wordlength
wordlength is a job to how many times words of a certain length occur. For example:

3 5  
5 10  
8 7  
10 2  

Means there are 5 words of length 3, 10 words of length 5, etc.

ant tasks:

* wordlength-clean
* wordlength-upload-input
* wordlength-compile
* wordlength-run
* wordlength-output

To run all at once:
wordlength

# ustrades
This job counts how much trading a country is conducting with the US. For example:

"Aphganistan" 1,003,402  
"Canada" 423,492,392  

Means Aphganistan has traded 1,003,402 units and Canada has traded 423,492,392 units.

ant tasks:

* ustrades-clean
* ustrades-upload-input // Does nothing due to size
* ustrades-compile
* ustrades-run
* ustrades-output

To run all at once:
ustrades