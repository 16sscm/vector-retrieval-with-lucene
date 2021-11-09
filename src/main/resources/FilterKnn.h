#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef FILTER_KNN_API_INNER
    #define FILTER_KNN_API __attribute__((visibility("default")))
#else
    #define FILTER_KNN_API
#endif

#ifndef BOOL
    #define BOOL int
#endif

FILTER_KNN_API const char * FilterKnn_GetErrorMsg();

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
FILTER_KNN_API BOOL FilterKnn_InitLibrary(size_t dimension,
                                          size_t numIvfCluster,
                                          size_t numPqSegments,
                                          size_t numPqBitsPerIdx,
                                          const char * pFlatFile,
                                          const char * pIvfpqFile);

FILTER_KNN_API BOOL FilterKnn_UnInitLibrary();

/**
 * @brief add vectors
 * 
 * @param[in] vectors vectors, n * dimension
 * @param[in] id vector id
 * @param[in] n 
 */
FILTER_KNN_API BOOL FilterKnn_AddVectors(const float * vectors, const int64_t * id, size_t n);

/**
 * @brief save index to file
 * 
 * @param[in] pFlatFile flat index file
 * @param[in] pIvfpqFile ivfpq index file
 */
FILTER_KNN_API BOOL FilterKnn_Save(const char * pFlatFile, const char * pIvfpqFile);

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
 * @return number of searched vectors, <= topK , < 0 means error
 */
FILTER_KNN_API int64_t FilterKnn_FlatSearch(const float * query,
                                            const int64_t * candidateIds,
                                            size_t numCandidateIds,
                                            float radius,
                                            size_t topK,
                                            int64_t * resultIds,
                                            float * resultDistances);

/**
 * @brief ivfpq search
 * 
 * @param[in] query query vector, dimension
 * @param[in] numSearchCluster max number of clusters to search, 0 means no limit
 * @param[in] numSearchVector max number of vectors to search, 0 means no limit
 * @param[in] maxZeroCluster max continuous zero cluster, 0 means no limit
 * @param[in] radius distance threshold, 0 means no limit
 * @param[in] topK 
 * @param[out] resultIds result id buffer, buffer size: topK
 * @param[out] resultDistances result distance buffer, buffer size: topK
 * @return number of searched vectors, <= topK , < 0 means error
 */
FILTER_KNN_API int64_t FilterKnn_IvfpqSearch(const float * query,
                                             size_t numSearchCluster,
                                             size_t numSearchVector,
                                             size_t maxZeroCluster,
                                             float radius,
                                             size_t topK,
                                             int64_t * resultIds,
                                             float * resultDistances);

#ifdef __cplusplus
}
#endif
