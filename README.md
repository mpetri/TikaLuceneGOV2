# A Tike-Lucene GOV2 parser

# Installation

```
git clone https://github.com/mpetri/TikaLuceneGOV2.git
mvn clean package appassembler:assemble
```

```
# D2SI Output format 

See description [here](https://github.com/ot/ds2i)

A binary sequence is a sequence of integers prefixed by its length, where both the sequence integers and the length are written as 32-bit little-endian unsigned integers.

A collection consists of 3 files, `gov2.docs`, `gov2.freqs`, `gov2.sizes`.

`gov2.docs` starts with a singleton binary sequence where its only integer is the number of documents in the collection. It is then followed by one binary sequence for each posting list, in order of term-ids. Each posting list contains the sequence of document-ids containing the term.

`gov2.freqs` is composed of a one binary sequence per posting list, where each sequence contains the occurrence counts of the postings, aligned with the previous file (note however that this file does not have an additional singleton list at its beginning).

`gov2.sizes` is composed of a single binary sequence whose length is the same as the number of documents in the collection, and the i-th element of the sequence is the size (number of terms) of the i-th document.

In addition the following files are created

`gov2.urls` the urls of the documents one per line.

`gov2.warcids` the unique warcids of the documents one per line.

`gov2.terms` terms based on the order which the lists occur in the index (UTF8-sort order)

# Creating the GOV2 collection

```
cd TikaLuceneGOV2
mvn clean package appassembler:assemble
./target/appassembler/bin/TikaLuceneWarc /path/to/GOV2 /tmp/lucene/index/path/ /GOV2-extras/URL2ID /GOV2-D2SI-OUTPUT-DIR/
```





