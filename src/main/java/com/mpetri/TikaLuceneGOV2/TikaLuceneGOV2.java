package com.mpetri.TikaLuceneGOV2;

import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.analysis.en.*;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import org.apache.logging.log4j.*;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.core.config.*;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.charset.StandardCharsets;
import static java.nio.file.FileVisitResult.*;
import static java.nio.file.FileVisitOption.*;
import java.util.*;

import java.util.zip.GZIPInputStream;
import java.util.concurrent.*;
import java.util.ArrayDeque;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.math3.util.Precision;
import java.lang.StringBuilder;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.apache.lucene.search.DocIdSetIterator;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public final class TikaLuceneGOV2 {

  public static final String FIELD_BODY = "contents";
  public static final String FIELD_ID = "id";
  public static final Pattern regexp1 = Pattern.compile(".*[a-zA-Z0-9].*");
  public static final Pattern regexp2 = Pattern.compile(".*[0-9]{10,}.*");
  public static final int MinDocSize = 5;

  public static class Posting implements Comparable<Posting> {
    public int id;
    public int freq;
    public Posting(int i, int s) {
      this.id = i;
      this.freq = s;
    }
    @Override
    public int compareTo(Posting anotherInstance) {
      return this.id - anotherInstance.id;
    }
  }

  public static class DocSize implements Comparable<DocSize> {
    public int id;
    public long size;
    public DocSize(int i, long s) {
      this.id = i;
      this.size = s;
    }
    public int compareTo(DocSize anotherInstance) {
      return this.id - anotherInstance.id;
    }
  }

  public final class Gov2Record {

    public static final String DOCNO = "<DOCNO>";
    public static final String TERMINATING_DOCNO = "</DOCNO>";

    public static final String DOC = "<DOC>";
    public static final String TERMINATING_DOC = "</DOC>";

    public static final String DOCHDR = "<DOCHDR>";
    public static final String TERMINATING_DOCHDR = "</DOCHDR>";

    public static final int BUFFER_SIZE = 1 << 18; // 64K
  }

  public static class indexFile implements Callable {
    private Path gov2File;
    private IndexWriter writer;
    private HtmlParser parser;
    public indexFile(Path gov2File, IndexWriter writer) {
      this.gov2File = gov2File;
      this.writer = writer;
      this.parser = new HtmlParser();
    }

    public static String flattenToAscii(String string) {
      char[] out = new char[string.length()];
      int j = 0;
      for (int i = 0, n = string.length(); i < n; ++i) {
        char c = string.charAt(i);
        if (c <= '\u007F')
          out[j++] = c;
      }
      return new String(out);
    }

    public Integer call() {
      int docCount = 0;
      try {
        InputStream stream = new GZIPInputStream(
            Files.newInputStream(gov2File, StandardOpenOption.READ),
            256 * 1024);
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(stream, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder(Gov2Record.BUFFER_SIZE);
        boolean found = false;
        for (;;) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }

          line = line.trim();

          if (line.startsWith(Gov2Record.DOC)) {
            found = true;
            continue;
          }

          if (line.startsWith(Gov2Record.TERMINATING_DOC)) {
            found = false;

            // (0) parse the GOV2 record
            int i = builder.indexOf(Gov2Record.DOCNO);
            if (i == -1)
              throw new RuntimeException("cannot find start tag " +
                                         Gov2Record.DOCNO);
            if (i != 0)
              throw new RuntimeException("should start with " +
                                         Gov2Record.DOCNO);
            int j = builder.indexOf(Gov2Record.TERMINATING_DOCNO);
            if (j == -1)
              throw new RuntimeException("cannot find end tag " +
                                         Gov2Record.TERMINATING_DOCNO);
            String docID =
                builder.substring(i + Gov2Record.DOCNO.length(), j).trim();
            i = builder.indexOf(Gov2Record.DOCHDR);
            if (i == -1)
              throw new RuntimeException("cannot find header tag " +
                                         Gov2Record.DOCHDR);
            j = builder.indexOf(Gov2Record.TERMINATING_DOCHDR);
            if (j == -1)
              throw new RuntimeException("cannot find end tag " +
                                         Gov2Record.TERMINATING_DOCHDR);
            if (j < i)
              throw new RuntimeException(Gov2Record.TERMINATING_DOCHDR +
                                         " comes before " + Gov2Record.DOCHDR);
            String content =
                builder.substring(j + Gov2Record.TERMINATING_DOCHDR.length())
                    .trim();
            builder.setLength(0);

            // (1) tika parse content
            try {
              InputStream tikastream =
                  new ByteArrayInputStream(content.getBytes());
              BodyContentHandler handler = new BodyContentHandler(-1);
              Metadata metadata = new Metadata();

              parser.parse(tikastream, handler, metadata);
              String parsedContent = handler.toString();

              // (2) more filtering on the parsed content
              parsedContent = flattenToAscii(parsedContent);
              parsedContent = parsedContent.replaceAll("\\s+", " ");
              String[] parts = parsedContent.split(" ");
              ArrayList<String> ok_parts = new ArrayList<String>();
              for (String tok : parts) {
                ok_parts.add(tok);
              }

              if (ok_parts.size() >= MinDocSize) {
                String finalContents = String.join(" ", ok_parts).trim();

                // (3) make a new, empty document
                Document document = new Document();

                // (4) add document id
                docID = docID.replaceAll("\\s+", "");
                document.add(new StringField(FIELD_ID, docID, Field.Store.YES));
                // (5) add document content
                FieldType fieldType = new FieldType();
                fieldType.setIndexOptions(
                    IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                fieldType.setStoreTermVectors(true);
                document.add(new Field(FIELD_BODY, finalContents, fieldType));
                // (6) index document content
                writer.addDocument(document);
                docCount++;
                continue;
              } else {
                System.err.println("FILTER " + docID);
              }
            } catch (TikaException e) {
              System.err.println("TIKA " + docID);
            } catch (IOException e) {
              System.err.println("TIKA " + docID);
            } catch (SAXException e) {
              System.err.println("TIKA " + docID);
            } catch (Exception e) {
              System.err.println("TIKA " + docID);
            }
          }

          if (found) {
            builder.append(line).append("\n");
          }
        }
      } catch (Exception e) {
        System.out.println(e);
      }
      return docCount;
    }
  }

  public static void writeIntLE(DataOutputStream out, int value)
      throws IOException {
    out.writeByte(value & 0xFF);
    out.writeByte((value >> 8) & 0xFF);
    out.writeByte((value >> 16) & 0xFF);
    out.writeByte((value >> 24) & 0xFF);
  }

  public static void main(String[] args) throws Exception {
    final Logger LOG = LogManager.getLogger(TikaLuceneGOV2.class);
    if (args.length != 4) {
      LOG.error("Insufficient command line parameters");
      System.out.println(
          "USAGE: TikaLuceneGOV2 <input gov2 dir> <output index dir> <gov2 url file> <d2si dir>");
      return;
    }

    String inputDir = args[0];
    String outputDir = args[1];
    String gov2URLFile = args[2];
    String d2siDir = args[3];
    String baseName = "gov2";
    LOG.info("inputDir =  " + inputDir);
    LOG.info("outputDir =  " + outputDir);
    LOG.info("gov2URLFile =  " + gov2URLFile);
    LOG.info("d2siDir =  " + d2siDir);

    // (1) determine input files
    final ArrayDeque<Path> inputFileStack = new ArrayDeque<>();
    final PathMatcher matcher =
        FileSystems.getDefault().getPathMatcher("glob:" + inputDir + "**/*.gz");
    Files.walkFileTree(Paths.get(inputDir), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        if (matcher.matches(file)) {
          inputFileStack.add(file);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc)
          throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });

    // (2) configure lucene index writer
    {
      Analyzer analyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          // (1) standard tokizer
          final Tokenizer source = new StandardTokenizer();

          // (2) lowercase everything
          TokenStream result = new StandardFilter(source);
          result = new EnglishPossessiveFilter(result);
          result = new LowerCaseFilter(result);

          // (3) split special words
          HashMap<String, String> arguments = new HashMap<String, String>();
          arguments.put("generateWordParts", "1");
          arguments.put("generateNumberParts", "1");
          arguments.put("catenateWords", "0");
          arguments.put("catenateNumbers", "0");
          arguments.put("catenateAll", "0");
          arguments.put("splitOnCaseChange", "1");
          arguments.put("splitOnNumerics", "1");
          WordDelimiterFilterFactory wordDelimiterFilterFactory =
              new WordDelimiterFilterFactory(arguments);
          result = wordDelimiterFilterFactory.create(result);

          // (4) run porter stemmer at the end
          result = new PorterStemFilter(result);

          return new TokenStreamComponents(source, result);
        }
      };

      final EnglishAnalyzer ea = new EnglishAnalyzer(CharArraySet.EMPTY_SET);
      final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setSimilarity(new BM25Similarity());
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      iwc.setRAMBufferSizeMB(1024);
      iwc.setUseCompoundFile(false);
      iwc.setMergeScheduler(new ConcurrentMergeScheduler());
      final Path indexPath = Paths.get(outputDir);
      final Directory indexDir = FSDirectory.open(indexPath);
      final IndexWriter writer = new IndexWriter(indexDir, iwc);

      // (3) iterate over all files and index
      final long start = System.nanoTime();
      int processors = Runtime.getRuntime().availableProcessors();
      ExecutorService es = Executors.newFixedThreadPool(processors);
      int numFiles = inputFileStack.size();
      LOG.info("threads = " + processors);
      LOG.info("files = " + numFiles);
      final ArrayDeque<Future> outputFutureStack = new ArrayDeque<>();
      for (Path gov2File : inputFileStack) {
        Future<Integer> future = es.submit(new indexFile(gov2File, writer));
        outputFutureStack.add(future);
      }

      int numProcessed = 0;
      for (Future<Integer> future : outputFutureStack) {
        try {
          Integer indexed_docs = future.get();
          numProcessed++;
          double percent = (double)numProcessed / (double)numFiles * 100;
          LOG.info("processed " + numProcessed + "/" + numFiles + " (" +
                   Precision.round(percent, 2) + "%) - " + indexed_docs);
        } catch (ExecutionException ex) {
          ex.getCause().printStackTrace();
        }
      }

      es.shutdown();
      try {
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        LOG.error("indexing files interrupted!");
        return;
      }

      // (4) merge into one index
      int numIndexed = writer.maxDoc();
      LOG.info("writing and merging indexes!");
      try {
        writer.commit();
        writer.forceMerge(1);
      } finally {
        writer.close();
      }
      final long durationMillis = TimeUnit.MILLISECONDS.convert(
          System.nanoTime() - start, TimeUnit.NANOSECONDS);
      LOG.info("Total " + numIndexed + " documents indexed in " +
               DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
    }
    {
      LOG.info("Parsing Lucene index from disk.");
      Path indexPath = Paths.get(outputDir);
      IndexReader reader;

      if (!Files.exists(indexPath) || !Files.isDirectory(indexPath) ||
          !Files.isReadable(indexPath)) {
        throw new IllegalArgumentException(
            outputDir + " does not exist or is not a directory.");
      }

      LOG.info("Reading index at " + outputDir);
      reader = DirectoryReader.open(FSDirectory.open(indexPath));

      int nDocs = reader.numDocs();
      LOG.info("Number of documents " + nDocs);
      HashMap<String, Integer> docMap = new HashMap();
      {
        LOG.info("Create document mapping.");
        for (int i = 0; i < nDocs; i++) {
          Document doc = reader.document(i);
          String docId = doc.get(FIELD_ID);
          Terms docTerms = reader.getTermVector(i, FIELD_BODY);
          if (docTerms != null) {
            docMap.put(docId, i);
          }
        }
        LOG.info("Found " + docMap.size() + " docids in index");
      }
      LOG.info("Parsing GOV2 URL mapping.");
      HashMap<Integer, Integer> urlMap = new HashMap();
      int urlRank = 0;
      try (BufferedReader br =
               new BufferedReader(new FileReader(gov2URLFile))) {
        for (String line; (line = br.readLine()) != null;) {
          int idx = line.indexOf(" ");
          String docID = line.substring(idx + 1);
          Integer id = docMap.get(docID);
          if (id != null) {
            urlMap.put(id, urlRank);
            urlRank++;
          }
        }
        LOG.info("Mapped " + urlMap.size() + " docids in index to URL order");
      }
      int realnDocs = urlMap.size();
      {
        LOG.info("Mapping postings lists.");
        Terms terms = SlowCompositeReaderWrapper.wrap(reader).terms("contents");

        String docsFile = d2siDir + "/" + baseName + ".docs";
        FileOutputStream docsfos = new FileOutputStream(docsFile);
        BufferedOutputStream bdocsfos =
            new BufferedOutputStream(docsfos, 64 * 1024 * 1024);
        DataOutputStream docsdos = new DataOutputStream(bdocsfos);
        writeIntLE(docsdos, 1);
        writeIntLE(docsdos, realnDocs);

        String freqsFile = d2siDir + "/" + baseName + ".freqs";
        FileOutputStream freqsfos = new FileOutputStream(freqsFile);
        BufferedOutputStream bfreqsfos =
            new BufferedOutputStream(freqsfos, 64 * 1024 * 1024);
        DataOutputStream freqsdos = new DataOutputStream(bfreqsfos);


        //String possFile = d2siDir + "/" + baseName + ".pos";
        //FileOutputStream possfos = new FileOutputStream(possFile);
        //BufferedOutputStream bpossfos =
        //    new BufferedOutputStream(possfos, 64 * 1024 * 1024);
        //DataOutputStream possdos = new DataOutputStream(bpossfos);

        String termsFile = d2siDir + "/" + baseName + ".terms";
        FileOutputStream termsfos = new FileOutputStream(termsFile);
        PrintWriter termsWriter = new PrintWriter(termsfos);

        TermsEnum termIter = terms.iterator();
        int numTerms = 0;
        int numTermsNoSingle = 0;
        long numPostings = 0;
        long numPostingsNoSingle = 0;
        int[] docSizes = new int[realnDocs];
        for (int i = 0; i < (docSizes.length); i++) {
          docSizes[i] = 0;
        }
        while (termIter.next() != null) {
          // for each posting
          String termStr = termIter.term().utf8ToString();
          int ft = termIter.docFreq();
          writeIntLE(docsdos, ft);
          writeIntLE(freqsdos, ft);

          ArrayList<Posting> postings = new ArrayList();

          DocsAndPositionsEnum docPosEnum =
              termIter.docsAndPositions(null, null);
          int ret = docPosEnum.nextDoc();
          while (ret !=
                 org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
            int dId = docPosEnum.docID();
            Integer mapped_id = urlMap.get(dId);
            if (mapped_id == null) {
              LOG.error("cannot find mapping for docid = " + dId);
            } else {
              int fdt = docPosEnum.freq();
              docSizes[mapped_id] += fdt;
              //ArrayList<Integer> positions = new ArrayList();
              //for (int i = 0; i < fdt; ++i) {
              //  final int position = docPosEnum.nextPosition();
              //  positions.add(position);
              //}
              postings.add(new Posting(mapped_id, fdt));
            }
            ret = docPosEnum.nextDoc();
          }

          Collections.sort(postings);
          for (Posting posting : postings) {
            writeIntLE(docsdos, posting.id);
            writeIntLE(freqsdos, posting.freq);

            //writeIntLE(possdos, posting.freq);
            //for (int i = 0; i < posting.freq; i++) {
            //  writeIntLE(possdos, posting.pos.get(i));
            //}
          }

          numPostings += ft;
          numTerms++;
          termsWriter.println(termStr);
          if (ft != 1) {
            numTermsNoSingle++;
            numPostingsNoSingle += ft;
          }
        }
        LOG.info("Number of terms " + numTerms);
        LOG.info("Number of postings " + numPostings);
        LOG.info("Number of terms (no single)" + numTermsNoSingle);
        LOG.info("Number of postings (no single)" + numPostingsNoSingle);
        freqsdos.close();
        docsdos.close();

        String sizeFile = d2siDir + "/" + baseName + ".sizes";
        LOG.info("Creating document size file " + sizeFile);
        FileOutputStream sizefos = new FileOutputStream(sizeFile);
        BufferedOutputStream bsizefos =
            new BufferedOutputStream(sizefos, 64 * 1024 * 1024);
        DataOutputStream sizedos = new DataOutputStream(bsizefos);
        writeIntLE(sizedos, realnDocs);
        LOG.info("writing " + realnDocs + " document sizes");
        for (int i = 0; i < (docSizes.length); i++) {
          writeIntLE(sizedos, docSizes[i]);
        }
        sizedos.close();
      }
    }
  }
}
