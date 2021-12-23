package com.hiretual.search.filterindex;

import com.hiretual.search.utils.GlobalPropertyUtils;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface CLib extends Library {
  String home = System.getProperty("user.home");
  String clibFile = GlobalPropertyUtils.get("c_lib_file");
  CLib INSTANCE = Native.loadLibrary(home + clibFile, CLib.class);
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
   * @param[in] dimensionPerPqSegment dimension of each PQ segment, recommend: 2
   * @param[in] numPqCluster PQ number of PQ cluster, recommend: 256
   * @param[in] pIvfpqFile ivfpq index file
   */
  int FilterKnn_InitLibrary(long dimension, long numIvfCluster,
                            long numPqqSegments, long numPqBitsPerIdx,
                            String pIvfpqFile);
  int FilterKnn_UnInitLibrary();
  /**
   * enable debug log
   * @param pLogPath log file path, log file path, if empty, output to console
   * @param rotationSize log file ratation size; if pLogPath is empty, ignored.
   * @param pChannelFilter log filter, if not empty, only the log of this
   *     channel will be output.
   * @param logLevel log level, only logs greater than or equal to this level
   *     will be output. value range: 0~5
   */
  void FilterKnn_SetDebugLog(String pLogPath, long rotationSize,
                             String pChannelFilter, long logLevel);

  /**
   * disable debug log
   */
  void ResetDebugLog();
  /**
   * remove vectors
   * @param[in] id vector id
   * @param[in] n
   */
  int FilterKnn_RemoveVectors(long[] id, long n);
  /**
   * @brief add vectors
   *
   * @param[in] vectors vectors, n * dimension
   * @param[in] id vector id
   * @param[in] n
   */
  int FilterKnn_AddVectors(float[] vectors, long[] id, long n,
                           String pJsonAttrs);
  /**
   * @brief save index to file
   *
   * @param[in] pFlatFile flat index file
   * @param[in] pIvfpqFile ivfpq index file
   */
  int FilterKnn_Save(String pIvfpqFile);
  /**
   * @brief flat search
   *
   * @param[in] query query vector, dimension
   * @param[in] candidateIds
   * @param[in] numCandidateIds
   * @param[in] radius distance threshold, 0 means no limit
   * @param[in] pJsonFilter filter description, json format
   * @param[in] topK
   * @param[out] resultIds result id buffer, buffer size: topK
   * @param[out] resultDistances result distance buffer, buffer size: topK
   * @return number of searched vectors, <= topK
   */
  long FilterKnn_FlatSearch(float[] query, long[] candidateIds,
                            long numCandidateIds, float radius,
                            String pJsonFilter, long topK, long[] resultIds,
                            float[] resultDistances);
  /**
   * @brief ivfpq search
   *
   * @param[in] query query vector, dimension
   * @param[in] numSearchCluster max number of clusters to search
   * @param[in] numSearchVector max number of vectors to search
   * @param[in] minZeroCluster min continuous zero cluster, 0 and 1 have the
   * @param[in] maxZeroCluster max continuous zero cluster, 0 means no limit.
   * same effect.
   * @param[in] radius distance threshold, 0 means no limit
   * @param[in] pJsonFilter filter description, json format
   * @param[in] topK
   * @param[out] resultIds result id buffer, buffer size: topK
   * @param[out] resultDistances result distance buffer, buffer size: topK
   * @return number of searched vectors, <= topK
   */
  long FilterKnn_IvfpqSearch(float[] query, long numSearchCluster,
                             long numSearchVector, long minZeroCluster,
                             long maxZeroCluster, float radius,
                             String pJsonFilter, long topK, long[] resultIds,
                             float[] resultDistances);

  Pointer FilterKnn_GetUids(long[] ids, long n);

  void FilterKnn_ReleaseStringArray(Pointer pointer);
}