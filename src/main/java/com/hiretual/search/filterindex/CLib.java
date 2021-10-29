package com.hiretual.search.filterindex;

import com.sun.jna.Library;
import com.sun.jna.Native;



public interface CLib extends Library {


    CLib INSTANCE = Native.loadLibrary("vector_search.so", CLib.class);
    String FilterKnn_GetErrorMsg();
    int FilterKnn_InitLibrary();
    int FilterKnn_UnInitLibrary();
    int FilterKnn_AddVectors(float[][]vectors,int[]id,int n);
    int FilterKnn_Save(String pFlatFile,String pIvfpqFile);
    int FilterKnn_FlatSearch(float[]vectors,int[]candidateIds,int numCandidateIds,int topK,int[]resultIds,float[] resultDistances);
    int FilterKnn_IvfpqSearch(float[]vectors,int topK,int[]resultIds,float[] resultDistances);

   
}