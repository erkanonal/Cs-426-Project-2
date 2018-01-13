#include <stdio.h>
#include <stdlib.h>

void getNumOfDocuments(char *W_name, int *num_of_documents);
void parseLine(char *line, int D, int i, int begin, int arr_documents[][D+1]);
void parseLineQuery(char *line, int *arr_query);
void read_documents(char *W_name, int D, int arr_documents[][D]);
void read_query(char *Q_name, int *arr_query);

