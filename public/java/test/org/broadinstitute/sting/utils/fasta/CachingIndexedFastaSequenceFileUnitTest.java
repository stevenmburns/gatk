// our package
package org.broadinstitute.sting.utils.fasta;


// the imports for unit testing.


import net.sf.picard.reference.IndexedFastaSequenceFile;
import net.sf.picard.reference.ReferenceSequence;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.log4j.Priority;
import org.broadinstitute.sting.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Basic unit test for GenomeLoc
 */
public class CachingIndexedFastaSequenceFileUnitTest extends BaseTest {
    private File simpleFasta = new File(publicTestDir + "/exampleFASTA.fasta");
    private static final int STEP_SIZE = 1;

    //private static final List<Integer> QUERY_SIZES = Arrays.asList(1);
    private static final List<Integer> QUERY_SIZES = Arrays.asList(1, 10, 100);
    private static final List<Integer> CACHE_SIZES = Arrays.asList(-1, 100, 1000);

    @DataProvider(name = "fastas")
    public Object[][] createData1() {
        List<Object[]> params = new ArrayList<Object[]>();
        for ( File fasta : Arrays.asList(simpleFasta) ) {
            for ( int cacheSize : CACHE_SIZES ) {
                for ( int querySize : QUERY_SIZES ) {
                    params.add(new Object[]{fasta, cacheSize, querySize});
                }
            }
        }

        return params.toArray(new Object[][]{});
    }

    private static long getCacheSize(final long cacheSizeRequested) {
        return cacheSizeRequested == -1 ? CachingIndexedFastaSequenceFile.DEFAULT_CACHE_SIZE : cacheSizeRequested;
    }

    @Test(dataProvider = "fastas", enabled = true)
    public void testCachingIndexedFastaReaderSequential1(File fasta, int cacheSize, int querySize) throws FileNotFoundException {
        final CachingIndexedFastaSequenceFile caching = new CachingIndexedFastaSequenceFile(fasta, getCacheSize(cacheSize));

        SAMSequenceRecord contig = caching.getSequenceDictionary().getSequence(0);
        logger.warn(String.format("Checking contig %s length %d with cache size %d and query size %d",
                contig.getSequenceName(), contig.getSequenceLength(), cacheSize, querySize));
        testSequential(caching, fasta, querySize);
    }

    private void testSequential(final CachingIndexedFastaSequenceFile caching, final File fasta, final int querySize) throws FileNotFoundException {
        final IndexedFastaSequenceFile uncached = new IndexedFastaSequenceFile(fasta);

        SAMSequenceRecord contig = uncached.getSequenceDictionary().getSequence(0);
        for ( int i = 0; i < contig.getSequenceLength(); i += STEP_SIZE ) {
            int start = i;
            int stop = start + querySize;
            if ( stop <= contig.getSequenceLength() ) {
                ReferenceSequence cachedVal = caching.getSubsequenceAt(contig.getSequenceName(), start, stop);
                ReferenceSequence uncachedVal = uncached.getSubsequenceAt(contig.getSequenceName(), start, stop);

                Assert.assertEquals(cachedVal.getName(), uncachedVal.getName());
                Assert.assertEquals(cachedVal.getContigIndex(), uncachedVal.getContigIndex());
                Assert.assertEquals(cachedVal.getBases(), uncachedVal.getBases());
            }
        }

        // asserts for efficiency.  We are going to make contig.length / STEP_SIZE queries
        // at each of range: start -> start + querySize against a cache with size of X.
        // we expect to hit the cache each time range falls within X.  We expect a hit
        // on the cache if range is within X.  Which should happen at least (X - query_size * 2) / STEP_SIZE
        // times.
        final int minExpectedHits = (int)Math.floor((Math.min(caching.getCacheSize(), contig.getSequenceLength()) - querySize * 2.0) / STEP_SIZE);
        caching.printEfficiency(Priority.WARN);
        Assert.assertTrue(caching.getCacheHits() >= minExpectedHits, "Expected at least " + minExpectedHits + " cache hits but only got " + caching.getCacheHits());

    }

    // Tests grabbing sequences around a middle cached value.
    @Test(dataProvider = "fastas", enabled = true)
    public void testCachingIndexedFastaReaderTwoStage(File fasta, int cacheSize, int querySize) throws FileNotFoundException {
        final IndexedFastaSequenceFile uncached = new IndexedFastaSequenceFile(fasta);
        final CachingIndexedFastaSequenceFile caching = new CachingIndexedFastaSequenceFile(fasta, getCacheSize(cacheSize));

        SAMSequenceRecord contig = uncached.getSequenceDictionary().getSequence(0);

        int middleStart = (contig.getSequenceLength() - querySize) / 2;
        int middleStop = middleStart + querySize;

        logger.warn(String.format("Checking contig %s length %d with cache size %d and query size %d with intermediate query",
                contig.getSequenceName(), contig.getSequenceLength(), cacheSize, querySize));

        for ( int i = 0; i < contig.getSequenceLength(); i += 10 ) {
            int start = i;
            int stop = start + querySize;
            if ( stop <= contig.getSequenceLength() ) {
                ReferenceSequence grabMiddle = caching.getSubsequenceAt(contig.getSequenceName(), middleStart, middleStop);
                ReferenceSequence cachedVal = caching.getSubsequenceAt(contig.getSequenceName(), start, stop);
                ReferenceSequence uncachedVal = uncached.getSubsequenceAt(contig.getSequenceName(), start, stop);

                Assert.assertEquals(cachedVal.getName(), uncachedVal.getName());
                Assert.assertEquals(cachedVal.getContigIndex(), uncachedVal.getContigIndex());
                Assert.assertEquals(cachedVal.getBases(), uncachedVal.getBases());
            }
        }
    }

    @DataProvider(name = "ParallelFastaTest")
    public Object[][] createParallelFastaTest() {
        List<Object[]> params = new ArrayList<Object[]>();
//        for ( int nt : Arrays.asList(1, 2, 3) ) {
//            for ( int cacheSize : CACHE_SIZES ) {
//                params.add(new Object[]{simpleFasta, cacheSize, 10, nt});
//            }
//        }

        for ( File fasta : Arrays.asList(simpleFasta) ) {
            for ( int cacheSize : CACHE_SIZES ) {
                for ( int querySize : QUERY_SIZES ) {
                    for ( int nt : Arrays.asList(1, 2, 3, 4) ) {
                        params.add(new Object[]{fasta, cacheSize, querySize, nt});
                    }
                }
            }
        }

        return params.toArray(new Object[][]{});
    }


    @Test(dataProvider = "ParallelFastaTest", enabled = true, timeOut = 60000)
    public void testCachingIndexedFastaReaderParallel(final File fasta, final int cacheSize, final int querySize, final int nt) throws FileNotFoundException, InterruptedException {
        final CachingIndexedFastaSequenceFile caching = new CachingIndexedFastaSequenceFile(fasta, getCacheSize(cacheSize));

        logger.warn(String.format("Parallel caching index fasta reader test cacheSize %d querySize %d nt %d", caching.getCacheSize(), querySize, nt));
        for ( int iterations = 0; iterations < 1; iterations++ ) {
            final ExecutorService executor = Executors.newFixedThreadPool(nt);
            final Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(nt);
            for ( int i = 0; i < nt; i++ )
                tasks.add(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        testSequential(caching, fasta, querySize);
                        return null;
                    }
                });
            executor.invokeAll(tasks);
            executor.shutdownNow();
        }
    }
}
