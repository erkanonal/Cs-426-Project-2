#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <math.h>
#include <time.h>
#include "utils.h"

//function prototypes
void kreduce(int *topk, int *myids, int *myvals, int k, int world_size, int my_rank);
void calculateSimilarity(int **myids, int **myvals, int work, int D, int documents[][D+1], int query[]);
int findMinIndex(int *arr);
void insertionSort(int topk[], int k, int myvals[]);
void reverse(int topk[], int myvals[], int k);

int main(int argc, char ** argv){
	MPI_Status status;
	int myid, numprocs;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);

	//if(argc <= 1)
	//	return 0;
	
	//char *W_name = "documents.txt";
	//char *Q_name = "query.txt";
	//char *W_name = "W_3";
	//char *Q_name = "Q";
	int D = 0;
	int num_of_documents = 0;
	int work = 0;

	int *topk;
	int *myids;
	int *myvals;
	int k;
	clock_t begin_time;
	clock_t end_time;
	double sequential_time;
	double parallel_time;
	double total_time;
	if(myid == 0){
		//start timer
		begin_time = clock();
		//args read
		D = atoi((char *)argv[1]);
		k = atoi((char *)argv[2]);
		char *W_name = (char *)argv[3];
		char *Q_name = (char *)argv[4];
	
		//read documents file
		getNumOfDocuments(W_name, &num_of_documents);
		int arr_documents[num_of_documents][D+1];

		//printf("num_of_documents: %d\n", num_of_documents);
		read_documents(W_name, D, arr_documents);
		//printf("%d\n", arr_documents[0][0]);

		//read query file
		int arr_query[D];
		//int *arr_query = malloc(sizeof(int)*D);
		read_query(Q_name, arr_query);

		//stop timer: end of the sequential part
		end_time = clock();
		sequential_time  = ((double)(end_time-begin_time))/CLOCKS_PER_SEC;
		printf("Sequential Part: %f ms\n", sequential_time);
		begin_time = clock();
		//root process divides the works for each process
		for(int i = 1; i < numprocs; i++){
			//printf("%d asdasd", arr_documents[0][0]);
			if(i == numprocs-1){
				int work = (num_of_documents / numprocs);	
				int work_extra = (num_of_documents % numprocs);
				int work_total = work + work_extra;
				int start = (i*work);	
				int end = ((i+1)*work) - 1 + work_extra;

				//printf("work_total: %d\n", work_total);
				//printf("start: %d\n", start);
				//printf("end: %d\n", end);
				//printf("\n");
				int documents[work][D+1];

				for(int a = 0, i = start; i < end+1; i++, a++){
					for(int b = 0, j = 0; j < D+1; j++, b++){
						documents[a][b] = arr_documents[i][j];
						//printf("%d aaaa\n", arr_documents[0][0]);	
						//printf("query[0] end: %d\n", arr_query[0]);
						//printf("%d", documents[a][b]);
					}
					//printf("\n");
				}
				//printf("\n");
				//send dictionary size D
				MPI_Send((void *)&D, 1, MPI_INT, i, 0XAAAA, MPI_COMM_WORLD);

				//send work
				MPI_Send((void *)&work_total, 1, MPI_INT, i, 0XBBBB, MPI_COMM_WORLD);

				//send documents	
				MPI_Send((void *)&documents, work_total*(D+1), MPI_INT, i, 0XCCCC, MPI_COMM_WORLD);

				//send query
				//printf("%d aaaa\n", arr_documents[0][0]);
				read_query(Q_name, arr_query);
				MPI_Send((void *)&arr_query, D, MPI_INT, i, 0XDDDD, MPI_COMM_WORLD);	
			}
			else{	
				int work = (num_of_documents/numprocs);	
				int start = (i*work);	
				int end = ((i+1)*work)-1;

				//printf("work: %d\n", work);
				//printf("start: %d\n", start);
				//printf("end: %d\n", end);

				int documents[work][D+1];
				for(int a = 0, i = start; i < end+1; i++, a++){
					for(int b = 0, j = 0; j < D+1; j++, b++){
						documents[a][b] = arr_documents[i][j];	
						//printf("%d aaaa\n", arr_documents[0][0]);					
						//printf("%d", documents[a][b]);
					}
					//printf("\n");
				}
				//send dictionary size D
				MPI_Send((void *)&D, 1, MPI_INT, i, 0XAAAA, MPI_COMM_WORLD);

				//send work
				MPI_Send((void *)&work, 1, MPI_INT, i, 0XBBBB, MPI_COMM_WORLD);

				//send documents	
				MPI_Send((void *)&documents, work*(D+1), MPI_INT, i, 0XCCCC, MPI_COMM_WORLD);

				//send query	
				MPI_Send((void *)&arr_query, D, MPI_INT, i, 0XDDDD, MPI_COMM_WORLD);	
			}
		}
		//printf("%d aaaa\n", arr_documents[0][0]);
		//printf("%d", documents[a][b]);	
		read_documents(W_name, D, arr_documents);
		//root does its work
		int work = (num_of_documents / numprocs);	
		int start = (myid*work);	
		int end = ((myid+1)*work)-1;
		
		int documents[work][D+1];
		for(int a = 0, i = start; i < end+1; i++, a++){
			for(int b = 0, j = 0; j < D+1; j++, b++){
				documents[a][b] = arr_documents[i][j];
				//printf("%d", documents[a][b]);
			}
			//printf("\n");
		}
		
		//malloc for myids and myvalues (+1 value allocated for -1 indicating end of the array)
		myids = malloc(sizeof(int)*(work+1));
		myvals = malloc(sizeof(int)*(work+1));
		myids[work] = -1;
		myvals[work] = -1;
		for(int i = 0; i < work; i++){
			myids[i] = documents[i][0];
			//printf("myid: %d ,myids[i]: %d\n", myid, myids[i]);
		}
		//calculate similarities	
			
		calculateSimilarity(&myids, &myvals, work, D, documents, arr_query);
		/*for(int i = 0; i < work; i++){
			myids[i] = documents[i][0];
			printf("proc num: %d sum: %d\n", (myids)[i], (myvals)[i]);
		}*/

	}
	
	if(myid != 0){
		//receive dictionary size D
		MPI_Recv((void *)&D, 1, MPI_INT, 0, 0XAAAA, MPI_COMM_WORLD, &status); 
		
		//receive work
		MPI_Recv((void *)&work, 1, MPI_INT, 0, 0XBBBB, MPI_COMM_WORLD, &status); 
		
		//receive documents
		int documents[work][D+1];
		MPI_Recv((void *)&documents, work*(D+1), MPI_INT, 0, 0XCCCC, MPI_COMM_WORLD, &status);
		if(myid == 3){
			for(int i = 0; i < work; i++){
				for(int j = 0; j < D+1; j++){
					//printf("%d", documents[i][j]);
				}
				//printf("id:%d\n", myid);
			}
		}

		//receive query
		int query[D];
		MPI_Recv((void *)&query, D, MPI_INT, 0, 0XDDDD, MPI_COMM_WORLD, &status);
		//printf("query[0] asda d: %d", query[0]);

		//malloc for myids and myvalues (+1 value allocated for -1 indicating end of the array)
		myids = malloc(sizeof(int)*(work+1));
		myvals = malloc(sizeof(int)*(work+1));
		myids[work] = -1;
		myvals[work] = -1;
		for(int i = 0; i < work; i++){
			myids[i] = documents[i][0];
			//printf("myid: %d ,myids[i]: %d\n", myid, myids[i]);
		}
		
		//calculate similarities
		
		calculateSimilarity(&myids, &myvals, work, D, documents, query);

		/*for(int i = 0; i < work; i++){
			myids[i] = documents[i][0];
			printf("proc num: %d sum: %d\n", (myids)[i], (myvals)[i]);
		}*/


	}
	
	//only root process uses topk so allocate space for him
	if(myid == 0){
		topk = malloc(sizeof(int)*(k+1));
		topk[k] = -1;
		int i = 0;
		while(topk[i] != -1){
			topk[i] = 0;
			i++;
		}
	}
	//every process calls kreduce
	kreduce(topk, myids, myvals, k, numprocs, myid);
	
	if(myid == 0){
		end_time = clock();
		parallel_time  = ((double)(end_time - begin_time))/CLOCKS_PER_SEC;
		total_time = (double)(sequential_time + parallel_time);
		printf("Parallel Part: %f ms\n", parallel_time);
		printf("Total time: %f ms\n", total_time);
		printf("Top k = %d ids:\n", k);
		int i = 0;
		while(topk[i] != -1){
			printf("%d\n", topk[i]);
			i++;
		}
	}
	
	//printf("%d ended\n", myid);
	MPI_Finalize();
}

void kreduce(int *topk, int *myids, int *myvals, int k, int world_size, int my_rank){
	if(my_rank == 0){
		//create temp value array for topk
		int value_topk[k+1];
		for(int i = 0; i < k+1; i++)
			value_topk[i] = 0;
		value_topk[k] = -1;
		//first root checks its own results
		int count1 = 0;
		while(topk[count1] != -1){
			int count2 = 0;
			while(myvals[count2] != -1){
				if(value_topk[count1] < myvals[count2]){
					int minIndex = findMinIndex(value_topk);
					value_topk[minIndex] = myvals[count2];
					topk[minIndex] = myids[count2];
					myvals[count2] = 0;
				}
				count2++;
				//printf("hey");
			}
			count1++;
		}
		//sort 0's own result
		/*int i = 0;
		while(value_topk[i] != -1){
			printf("here topk: %d value_topk: %d\n", (topk)[i], (value_topk)[i]);
			i++;
		}*/
		insertionSort(topk, k, value_topk);
		/*int i = 0;
		while(value_topk[i] != -1){
			printf("here topk: %d value_topk: %d\n", (topk)[i], (value_topk)[i]);
			i++;
		}*/
		//get results from other processors
		for(int i = 1; i < world_size; i++){
			MPI_Status status;
			int arr_size = 0;
			
			//receive arr_size first
			MPI_Recv((void *)&arr_size, 1, MPI_INT, i, 0XAAAB, MPI_COMM_WORLD, &status);
			//printf("id: %d, arr_size: %d\n", i, arr_size);	
						
			//receive id array and value array
			int other_myids[arr_size];
			int other_myvals[arr_size];			
			MPI_Recv((void *)&other_myids, arr_size, MPI_INT, i, 0XAABB, MPI_COMM_WORLD, &status);
			MPI_Recv((void *)&other_myvals, arr_size, MPI_INT, i, 0XABBB, MPI_COMM_WORLD, &status);
			
			/*printf("\n");
			int i = 0;
			while(other_myids[i] != -1){
				printf("here other_myids: %d other_myvals: %d\n", (other_myids)[i], (other_myvals)[i]);
				i++;
			}*/
			/*while(value_topk[i] != -1){
				printf("here topk: %d value_topk: %d\n", (topk)[i], (value_topk)[i]);
				i++;
			}
			printf("\n");*/
			//comparison part
			int count1 = 0;
			while(topk[count1] != -1){
				int count2 = 0;
				while(other_myvals[count2] != -1){
					if(value_topk[count1] < other_myvals[count2]){
						int minIndex = findMinIndex(value_topk);
						value_topk[minIndex] = other_myvals[count2];
						topk[minIndex] = other_myids[count2];
						other_myvals[count2] = 0;
					}
					count2++;
				}
				count1++;
			}
			//sort the final results
			insertionSort(topk, k, value_topk);
		}
	}
	else{
		//if I am not root process, then send my results to root
		int arr_size = 0;
		while(myids[arr_size] != -1)
			arr_size++;
		arr_size++;
		//send arr_size first
		MPI_Send((void *)&arr_size, 1, MPI_INT, 0, 0XAAAB, MPI_COMM_WORLD);

		//send myids array
		int myids_arr[arr_size];
		myids_arr[arr_size-1] = -1;
		for(int i = 0; i < arr_size-1; i++){
			myids_arr[i] = myids[i];
			
		}
		MPI_Send((void *)&myids_arr, arr_size, MPI_INT, 0, 0XAABB, MPI_COMM_WORLD);

		//send myvals array
		int myvals_arr[arr_size];
		myvals_arr[arr_size-1] = -1;
		for(int i = 0; i < arr_size-1; i++){
			myvals_arr[i] = myvals[i];
			
		}
		MPI_Send((void *)&myvals_arr, arr_size, MPI_INT, 0, 0XABBB, MPI_COMM_WORLD);
	}
}

void insertionSort(int topk[], int k, int myvals[])
{		
	int i, key1, key2, j;
	for (i = 1; myvals[i] != -1; i++){
		key1 = myvals[i];
		key2 = topk[i];
		j = i-1;

		while (j >= 0 && myvals[j] > key1)
		{
		   myvals[j+1] = myvals[j];
		   topk[j+1] = topk[j];
		   j = j-1;
		}
		myvals[j+1] = key1;
		topk[j+1] = key2;
	}
	//after sorting reverse	
	reverse(topk, myvals, k);
}

void reverse(int topk[], int myvals[], int k){
	int i = 0;
	int j = k-1;
	while(i < j){
		//reverse indexes
		int temp = topk[i];
		topk[i] = topk[j];
		topk[j] = temp;

		//reverse values
		temp = myvals[i];
		myvals[i] = myvals[j];
		myvals[j] = temp;
		i++;
		j--;

	}
}

int findMinIndex(int arr[]){
	int minIndex = 0;
	int i = 0;
	while(arr[i] != -1){
		if(arr[i] < arr[minIndex])
			minIndex = i;
		i++;
	}
	return minIndex;
}
void calculateSimilarity(int **myids, int **myvals, int work, int D, int documents[][D+1], int query[]){
	for(int i = 0; i < work; i++){
		int sum = 0;
		for(int j = 0; j < D; j++){
			//printf("sum: %d\n", (int)(pow(documents[i][j+1], query[j])));
			sum = sum + (int)(pow(documents[i][j+1], query[j]));
		}
		//printf("\n");
		//printf("myids: %d sum: %d\n", (*myids)[i], sum);
		(*myvals)[i] = sum;
	}
}
