package org.broadinstitute.sting.gatk.walkers.varianteval;

import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.utils.genotype.Variation;

import java.util.ArrayList;
import java.util.List;

/**
 * The Broad Institute
 * SOFTWARE COPYRIGHT NOTICE AGREEMENT
 * This software and its documentation are copyright 2009 by the
 * Broad Institute/Massachusetts Institute of Technology. All rights are reserved.
 * <p/>
 * This software is supplied without any warranty or guaranteed support whatsoever. Neither
 * the Broad Institute nor MIT can be responsible for its use, misuse, or functionality.
 */
public class IndelMetricsAnalysis extends BasicVariantAnalysis implements GenotypeAnalysis, PopulationAnalysis {
    long insertions = 0;
    long deletions = 0;
    int maxSize = 0;
    long[][] sizes = new long[2][100];

    public IndelMetricsAnalysis() {
        super("indel_metrics");
        for (int i = 0; i < 100; i++)
            sizes[0][i] = sizes[1][i] = 0;
    }

    public String update(Variation eval, RefMetaDataTracker tracker, char ref, AlignmentContext context) {
        if (eval != null && eval.isInsertion()) {
            if (eval.isInsertion())
                insertions++;
            else if (eval.isDeletion())
                deletions++;
            else
                throw new RuntimeException("Variation is indel, but isn't insertion or deletion!");

            for (String allele : eval.getAlleleList())
                if (allele.length() < 100) {
                    sizes[eval.isDeletion() ? 0 : 1][allele.length()]++;
                    if (allele.length() > maxSize)
                        maxSize = allele.length();
                }
        }

        return null;
    }

    public List<String> done() {
        List<String> s = new ArrayList<String>();
        s.add(String.format("total_deletions       %d", deletions));
        s.add(String.format("total_insertions      %d", insertions));
        s.add(String.format(""));

        s.add("Size Distribution");
        s.add("size\tdeletions\tinsertions");
        for (int i = 1; i <= maxSize; i++)
            s.add(String.format("%d\t%d\t\t%d", i, sizes[0][i], sizes[1][i]));

        return s;
    }
}