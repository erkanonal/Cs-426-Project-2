#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "string.h"
#include "utils.h"

void getNumOfDocuments(char *W_name, int *num_of_documents){
	int ch;

	FILE *file;
	file = fopen(W_name, "r");

	do{
		ch = fgetc(file);
		if(ch == '\n')
		(*num_of_documents)++;			//determine the number of documents
	}while(ch != EOF);

	fclose(file);
}
void read_documents(char *W_name, int D, int arr_documents[][D+1]) {
	//printf("read_documents\n");
	char * line = NULL;
	size_t len = 0;
        ssize_t read;
	int ch;

	FILE *file;
	file = fopen(W_name, "r");

	int i = 0;
	while ((read = getline(&line, &len, file)) != -1) {
		//parse the line
		//printf("line is: %s\n", line);

		int start = 0;
		int end = 0;
		while(line[end] != ':')
			end++;
		char id_str[end-start+1]; 
		strncpy(id_str, line+start, end-start);
		id_str[end-start] = '\0';

		int id_int = atoi(id_str);
		arr_documents[i][0] = id_int;

		//parse each line for getting input correctly
		parseLine(line, D, i, end+2, arr_documents);
		i++;		
   	}
	fclose(file);
}

void parseLine(char *line, int D, int i, int begin, int arr_documents[][D+1]){
	int j = begin;

	int k = 1;
	int start;
	int end;
	while(line[j] != '\n'){
		start = j;
		end = j;
		while(line[end] != ' ' && line[end] != '\n')
			end++;
		char str[end-start+1]; 
		strncpy(str, line+start, end-start);
		str[end-start] = '\0';
		//printf("%s\n", otherString);

		int test = atoi(str);
		//printf("%d\n", test);

		arr_documents[i][k] = test;
		k++;

		if(line[end] == '\n')
			j = end;
		else
			j = end+1;
	}
}

void read_query(char *Q_name, int arr_query[]) {
	//printf("read_query\n");
	char * line = NULL;
	size_t len = 0;
        ssize_t read;
	int ch;

	FILE *file;
	file = fopen(Q_name, "r");

	int i = 0;
	while ((read = getline(&line, &len, file)) != -1) {
		//parse the line
		//printf("line is: %s\n", line);

		//parse each line for getting input correctly
		parseLineQuery(line, arr_query);
		i++;		
   	}
	fclose(file);
}

void parseLineQuery(char *line, int arr_query[]){
	int j = 0;
	int k = 0;
	int start;
	int end;
	while(line[j] != '\n'){
		start = j;
		end = j;
		while(line[end] != ' ' && line[end] != '\n')
			end++;
		char str[end-start+1]; 
		strncpy(str, line+start, end-start);
		str[end-start] = '\0';
		//printf("%s\n", line);

		int test = atoi(str);
		//printf("%d\n", test);

		arr_query[k] = test;
		k++;

		if(line[end] == '\n')
			j = end;
		else
			j = end+1;
	}
}
