package com.hiretual.search.filterindex;

import com.sun.jna.Library;
import com.sun.jna.Native;



public interface CLib extends Library {


    CLib INSTANCE = Native.loadLibrary("vector_search.so", CLib.class);
    String FilterKnn_GetErrorMsg();
    /**
 * @brief Init knn library
 * 
 * @param[in] dimension vector dimension
 * @param[in] numIvfCluster number of ivf cluster
 * @param[in] numPqqSegments number of PQ segment
 * @param[in] numPqBitsPerIdx number of bits required for store one PQ segment. At present, it can only be 8
 * @param[in] pFlatFile flat index file
 * @param[in] pIvfpqFile ivfpq index file
 */
    int FilterKnn_InitLibrary();
    int FilterKnn_UnInitLibrary();
    /**
 * @brief add vectors
 * 
 * @param[in] vectors vectors, n * dimension
 * @param[in] id vector id
 * @param[in] n 
 */
    int FilterKnn_AddVectors(float[][]vectors,int[]id,int n);
    int FilterKnn_Save(String pFlatFile,String pIvfpqFile);
    int FilterKnn_FlatSearch(float[]vectors,int[]candidateIds,int numCandidateIds,int topK,int[]resultIds,float[] resultDistances);
    int FilterKnn_IvfpqSearch(float[]vectors,int topK,int[]resultIds,float[] resultDistances);

   
}