package com.hiretual.search.filterindex;

import com.hiretual.search.utils.GlobalPropertyUtils;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;


public interface CLib extends Library {
   String home=System.getProperty("user.home");
   String clibFile=GlobalPropertyUtils.get("c_lib_file");
   CLib INSTANCE = Native.loadLibrary(home+clibFile, CLib.class);
    /**
     * get the error message
     * @return error String
     */
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
    int FilterKnn_InitLibrary(long dimension,long numIvfCluster,long numPqqSegments,long numPqBitsPerIdx,String pFlatFile,String pIvfpqFile );
    int FilterKnn_UnInitLibrary();
    /**
 * @brief add vectors
 * 
 * @param[in] vectors vectors, n * dimension
 * @param[in] id vector id
 * @param[in] n 
 */
    int FilterKnn_AddVectors(float[]vectors,long[]id,long n);
    /**
 * @brief save index to file
 * 
 * @param[in] pFlatFile flat index file
 * @param[in] pIvfpqFile ivfpq index file
 */
    int FilterKnn_Save(String pFlatFile,String pIvfpqFile);
    /**
 * @brief flat search
 * 
 * @param[in] query query vector, dimension
 * @param[in] candidateIds 
 * @param[in] numCandidateIds 
 * @param[in] radius distance threshold, 0 means no limit
 * @param[in] topK 
 * @param[out] resultIds result id buffer, buffer size: topK
 * @param[out] resultDistances result distance buffer, buffer size: topK
 * @return number of searched vectors, <= topK 
 */
    long FilterKnn_FlatSearch(float[]query,long[]candidateIds,long numCandidateIds,float radius,long topK,long[]resultIds,float[] resultDistances);
    /**
 * @brief ivfpq search
 * 
 * @param[in] query query vector, dimension
 * @param[in] numSearchCluster max number of clusters to search
 * @param[in] numSearchVector max number of vectors to search
 * @param[in] maxZeroCluster max continuous zero cluster, 0 means no limit
 * @param[in] radius distance threshold, 0 means no limit
 * @param[in] topK 
 * @param[out] resultIds result id buffer, buffer size: topK
 * @param[out] resultDistances result distance buffer, buffer size: topK
 * @return number of searched vectors, <= topK 
 */
    long FilterKnn_IvfpqSearch(float[]query,long numSearchCluster,long numSearchVector,long maxZeroCluster, float radius,long topK,long[]resultIds,float[] resultDistances);

    Pointer FilterKnn_TestStringArray(String[]input , long n);
    void FilterKnn_ReleaseStringArray(Pointer pointer);
}