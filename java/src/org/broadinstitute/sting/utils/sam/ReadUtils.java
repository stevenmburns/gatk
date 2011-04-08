/*
 * Copyright (c) 2010 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.utils.sam;

import net.sf.samtools.*;
import org.broadinstitute.sting.utils.collections.Pair;
import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;

import java.util.*;
import java.io.File;

/**
 * A miscellaneous collection of utilities for working with SAM files, headers, etc.
 * Static methods only, please.
 *
 * @author mhanna
 * @version 0.1
 */
public class ReadUtils {
    private ReadUtils() { }

    public static SAMFileHeader copySAMFileHeader(SAMFileHeader toCopy) {
        SAMFileHeader copy = new SAMFileHeader();

        copy.setSortOrder(toCopy.getSortOrder());
        copy.setGroupOrder(toCopy.getGroupOrder());
        copy.setProgramRecords(toCopy.getProgramRecords());
        copy.setReadGroups(toCopy.getReadGroups());
        copy.setSequenceDictionary(toCopy.getSequenceDictionary());

        for (Map.Entry<String, String> e : toCopy.getAttributes())
            copy.setAttribute(e.getKey(), e.getValue());

        return copy;
    }

    public static SAMFileWriter createSAMFileWriterWithCompression(SAMFileHeader header, boolean presorted, String file, int compression) {
        if (file.endsWith(".bam"))
            return new SAMFileWriterFactory().makeBAMWriter(header, presorted, new File(file), compression);
        return new SAMFileWriterFactory().makeSAMOrBAMWriter(header, presorted, new File(file));
    }

    public static boolean isPlatformRead(SAMRecord read, String name) {
        SAMReadGroupRecord readGroup = read.getReadGroup();
        if (readGroup != null) {
            Object readPlatformAttr = readGroup.getAttribute("PL");
            if (readPlatformAttr != null)
                return readPlatformAttr.toString().toUpperCase().contains(name);
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------------------------
    //
    // utilities for detecting overlapping reads
    //
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Detects read pairs where the reads are so long relative to the over fragment size that they are
     * reading into each other's adaptors.
     *
     * Normally, fragments are sufficiently far apart that reads aren't reading into each other.
     *
     * |-------------------->                                   first read
     *                                 <--------------------|   second read
     *
     * Sometimes, mostly due to lab errors or constraints, fragment library are made too short relative to the
     * length of the reads.  For example, it's possible to have 76bp PE reads with 125 bp inserts, so that ~25 bp of each
     * read overlaps with its mate.
     *
     * |--------OOOOOOOOOOOO>               first read
     *         <OOOOOOOOOOOO------------|   second read
     *
     * This filter deals with the situation where the fragment is so small that the each read actually reads into the
     * adaptor sequence of its mate, generating mismatches at both ends of the read:
     *
     *              |----------------XXXX>      first read
     *         <XXXX----------------|           second read
     *
     * The code below returns NOT_OVERLAPPING for the first case, IN_ADAPTOR for the last case, and OVERLAPPING
     * given a read and a reference aligned base position.
     *
     * @author depristo
     * @version 0.1
     */

    public enum OverlapType { NOT_OVERLAPPING, IN_ADAPTOR }

    /**
     * God, there's a huge information asymmetry in SAM format:
     *
     *      s1                      e1
     *      |-----------------------> [record in hand]
     *  s2
     *  <-----------------------|
     *
     * s1, e1, and s2 are all in the record.  From isize we can can compute e2 as s1 + isize + 1
     *
     *      s2
     *      |----------------------->
     *  s1                      e1
     *  <-----------------------|     [record in hand]
     *
     * Here we cannot calculate e2 since the record carries s2 and e1 + isize is s2 now!
     *
     * This makes the following code a little nasty, since we can only detect if a base is in the adaptor, but not
     * if it overlaps the read.
     *
     * @param rec
     * @param basePos
     * @param adaptorLength
     * @return
     */
    public static OverlapType readPairBaseOverlapType(final SAMRecord rec, long basePos, final int adaptorLength) {
        OverlapType state = OverlapType.NOT_OVERLAPPING;

        Pair<Integer, Integer> adaptorBoundaries = getAdaptorBoundaries(rec, adaptorLength);

        if ( adaptorBoundaries != null ) { // we're not an unmapped pair -- cannot filter out

            boolean inAdapator = basePos >= adaptorBoundaries.first && basePos <= adaptorBoundaries.second;

            if ( inAdapator ) { 
                state = OverlapType.IN_ADAPTOR;
                //System.out.printf("baseOverlapState: %50s negStrand=%b base=%d start=%d stop=%d, adaptorStart=%d adaptorEnd=%d isize=%d => %s%n",
                //        rec.getReadName(), rec.getReadNegativeStrandFlag(), basePos, rec.getAlignmentStart(), rec.getAlignmentEnd(), adaptorBoundaries.first, adaptorBoundaries.second, rec.getInferredInsertSize(), state);
            }
        }

        return state;
    }

    private static Pair<Integer, Integer> getAdaptorBoundaries(SAMRecord rec, int adaptorLength) {
        int isize = rec.getInferredInsertSize();
        if ( isize == 0 )
            return null; // don't worry about unmapped pairs

        int adaptorStart, adaptorEnd;

        if ( rec.getReadNegativeStrandFlag() ) {
            // we are on the negative strand, so our mate is on the positive strand
            int mateStart = rec.getMateAlignmentStart();
            adaptorStart = mateStart - adaptorLength - 1;
            adaptorEnd = mateStart - 1;
        } else {
            // we are on the positive strand, so our mate is on the negative strand
            int mateEnd = rec.getAlignmentStart() + isize - 1;
            adaptorStart = mateEnd + 1;
            adaptorEnd = mateEnd + adaptorLength;
        }

        return new Pair<Integer, Integer>(adaptorStart, adaptorEnd);
    }

    // return true if the read needs to be completely clipped
    public static boolean hardClipAdaptorSequence(final SAMRecord rec, int adaptorLength) {

        Pair<Integer, Integer> adaptorBoundaries = getAdaptorBoundaries(rec, adaptorLength);
        if ( adaptorBoundaries != null ) {
            if ( rec.getReadNegativeStrandFlag() && adaptorBoundaries.second >= rec.getAlignmentStart() && adaptorBoundaries.first < rec.getAlignmentEnd() )
                return hardClipStartOfRead(rec, adaptorBoundaries.second);
            else if ( !rec.getReadNegativeStrandFlag() && adaptorBoundaries.first <= rec.getAlignmentEnd() )
                return hardClipEndOfRead(rec, adaptorBoundaries.first);
        }

        return false;
    }

    // return true if the read needs to be completely clipped
    private static boolean hardClipStartOfRead(SAMRecord rec, int stopPosition) {

        if ( stopPosition >= rec.getAlignmentEnd() ) {
            // BAM representation issue -- we can't clip away all bases in a read, just leave it alone and let the filter deal with it
            //System.out.printf("Entire read needs to be clipped: %50s %n", rec.getReadName());
            return true;
        }

        //System.out.printf("Clipping start of read: %50s start=%d adaptorEnd=%d isize=%d %n",
        //        rec.getReadName(), rec.getAlignmentStart(), stopPosition, rec.getInferredInsertSize());

        Cigar oldCigar = rec.getCigar();
        LinkedList<CigarElement> newCigarElements = new LinkedList<CigarElement>();
        int currentPos = rec.getAlignmentStart();
        int basesToClip = 0;
        int basesAlreadyClipped = 0;

        for ( CigarElement ce : oldCigar.getCigarElements() ) {

            if ( currentPos > stopPosition) {
                newCigarElements.add(ce);
                continue;
            }

            int elementLength = ce.getLength();
            switch ( ce.getOperator() ) {
                case M:
                    for (int i = 0; i < elementLength; i++, currentPos++, basesToClip++) {
                        if ( currentPos > stopPosition ) {
                            newCigarElements.add(new CigarElement(elementLength - i, CigarOperator.M));
                            break;
                        }
                    }
                    break;
                case I:
                case S:
                    basesToClip += elementLength;
                    break;
                case D:
                case N:
                    currentPos += elementLength;
                    break;
                case H:
                    basesAlreadyClipped += elementLength;
                case P:
                    break;
                default: throw new ReviewedStingException("The " + ce.getOperator() + " cigar element is not currently supported");
            }

        }

        // copy over the unclipped bases
        final byte[] bases = rec.getReadBases();
        final byte[] quals = rec.getBaseQualities();
        int newLength = bases.length - basesToClip;
        byte[] newBases = new byte[newLength];
        byte[] newQuals = new byte[newLength];
        System.arraycopy(bases, basesToClip, newBases, 0, newLength);
        System.arraycopy(quals, basesToClip, newQuals, 0, newLength);
        rec.setReadBases(newBases);
        rec.setBaseQualities(newQuals);

        // now add a CIGAR element for the clipped bases
        newCigarElements.addFirst(new CigarElement(basesToClip + basesAlreadyClipped, CigarOperator.H));
        Cigar newCigar = new Cigar(newCigarElements);
        rec.setCigar(newCigar);

        // adjust the start accordingly
        rec.setAlignmentStart(stopPosition + 1);

        return false;
    }

    // return true if the read needs to be completely clipped
    private static boolean hardClipEndOfRead(SAMRecord rec, int startPosition) {

        if ( startPosition <= rec.getAlignmentStart() ) {
            // BAM representation issue -- we can't clip away all bases in a read, just leave it alone and let the filter deal with it
            //System.out.printf("Entire read needs to be clipped: %50s %n", rec.getReadName());
            return true;
        }

        //System.out.printf("Clipping end of read: %50s adaptorStart=%d end=%d isize=%d %n",
        //        rec.getReadName(), startPosition, rec.getAlignmentEnd(), rec.getInferredInsertSize());

        Cigar oldCigar = rec.getCigar();
        LinkedList<CigarElement> newCigarElements = new LinkedList<CigarElement>();
        int currentPos = rec.getAlignmentStart();
        int basesToKeep = 0;
        int basesAlreadyClipped = 0;

        for ( CigarElement ce : oldCigar.getCigarElements() ) {

            int elementLength = ce.getLength();

            if ( currentPos >= startPosition ) {
                if ( ce.getOperator() == CigarOperator.H )
                    basesAlreadyClipped += elementLength;
                continue;
            }

            switch ( ce.getOperator() ) {
                case M:
                    for (int i = 0; i < elementLength; i++, currentPos++, basesToKeep++) {
                        if ( currentPos == startPosition ) {
                            newCigarElements.add(new CigarElement(i, CigarOperator.M));
                            break;
                        }
                    }

                    if ( currentPos != startPosition )
                        newCigarElements.add(ce);
                    break;
                case I:
                case S:
                    newCigarElements.add(ce);
                    basesToKeep += elementLength;
                    break;
                case D:
                case N:
                    newCigarElements.add(ce);
                    currentPos += elementLength;
                    break;
                case H:
                case P:
                    newCigarElements.add(ce);
                    break;
                default: throw new ReviewedStingException("The " + ce.getOperator() + " cigar element is not currently supported");
            }

        }

        // copy over the unclipped bases
        final byte[] bases = rec.getReadBases();
        final byte[] quals = rec.getBaseQualities();
        byte[] newBases = new byte[basesToKeep];
        byte[] newQuals = new byte[basesToKeep];
        System.arraycopy(bases, 0, newBases, 0, basesToKeep);
        System.arraycopy(quals, 0, newQuals, 0, basesToKeep);
        rec.setReadBases(newBases);
        rec.setBaseQualities(newQuals);

        // now add a CIGAR element for the clipped bases
        newCigarElements.add(new CigarElement((bases.length - basesToKeep) + basesAlreadyClipped, CigarOperator.H));
        Cigar newCigar = new Cigar(newCigarElements);
        rec.setCigar(newCigar);

        // adjust the stop accordingly
        // rec.setAlignmentEnd(startPosition - 1);

        return false;
    }

    private static int DEFAULT_ADAPTOR_SIZE = 100;

    public static boolean hardClipAdaptorSequence(final SAMRecord rec) {
        return hardClipAdaptorSequence(rec, DEFAULT_ADAPTOR_SIZE);
    }

    public static OverlapType readPairBaseOverlapType(final SAMRecord rec, long basePos) {
        return readPairBaseOverlapType(rec, basePos, DEFAULT_ADAPTOR_SIZE);
    }

    public static boolean is454Read(SAMRecord read) {
        return isPlatformRead(read, "454");
    }

    public static boolean isSOLiDRead(SAMRecord read) {
        return isPlatformRead(read, "SOLID");
    }

    public static boolean isSLXRead(SAMRecord read) {
        return isPlatformRead(read, "ILLUMINA");
    }

    private static final Map<Integer, String> readFlagNames
            = new HashMap<Integer, String>();

    static {
        readFlagNames.put(0x1, "Paired");
        readFlagNames.put(0x2, "Proper");
        readFlagNames.put(0x4, "Unmapped");
        readFlagNames.put(0x8, "MateUnmapped");
        readFlagNames.put(0x10, "Forward");
        //readFlagNames.put(0x20, "MateForward");
        readFlagNames.put(0x40, "FirstOfPair");
        readFlagNames.put(0x80, "SecondOfPair");
        readFlagNames.put(0x100, "NotPrimary");
        readFlagNames.put(0x200, "NON-PF");
        readFlagNames.put(0x400, "Duplicate");
    }

    public static String readFlagsAsString(SAMRecord rec) {
        String flags = "";
        for (int flag : readFlagNames.keySet()) {
            if ((rec.getFlags() & flag) != 0) {
                flags += readFlagNames.get(flag) + " ";
            }
        }
        return flags;
    }

    /**
     * Returns the collections of reads sorted in coordinate order, according to the order defined
     * in the reads themselves
     *
     * @param reads
     * @return
     */
    public final static void coordinateSortReads(List<SAMRecord> reads) {
        final SAMRecordComparator comparer = new SAMRecordCoordinateComparator();
        Collections.sort(reads, comparer);
    }

    public final static int getFirstInsertionOffset(SAMRecord read) {
        CigarElement e = read.getCigar().getCigarElement(0);
        if ( e.getOperator() == CigarOperator.I )
            return e.getLength();
        else
            return 0;
    }

    public final static int getLastInsertionOffset(SAMRecord read) {
        CigarElement e = read.getCigar().getCigarElement(read.getCigarLength()-1);
        if ( e.getOperator() == CigarOperator.I )
            return e.getLength();
        else
            return 0;
    }




}
