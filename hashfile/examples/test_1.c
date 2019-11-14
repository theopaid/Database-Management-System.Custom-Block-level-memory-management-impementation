#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 1700	// Changeable
#define BUCKETS_NUM 13		// Changeable
#define FILE_NAME "data_test_1.db"

const char* names[] = {
	"Yannis",
	"Christoforos",
	"Sofia",
	"Marianna",
	"Vagelis",
	"Maria",
	"Iosif",
	"Dionisis",
	"Konstantina",
	"Theofilos",
	"Giorgos",
	"Dimitris"
};

const char* surnames[] = {
	"Ioannidis",
	"Svingos",
	"Karvounari",
	"Rezkalla",
	"Nikopoulos",
	"Berreta",
	"Koronis",
	"Gaitanis",
	"Oikonomou",
	"Mailis",
	"Michas",
	"Halatsis"
};

const char* cities[] = {
	"Athens",
	"San Francisco",
	"Los Angeles",
	"Amsterdam",
	"London",
	"New York",
	"Tokyo",
	"Honk Kong",
	"Munich",
	"Miami"
};

#define CALL_OR_DIE(call)	  \
 {				  \
	HT_ErrorCode code = call; \
	if (code != HT_OK) {	  \
		printf("Error\n");\
		exit(code);	  \
	}			  \
 }

int main() {
	// Testing some hashing techniques/functions
	//
	// Create and Open
	
	int fd1;
	int fd2;
	char *data;
	BF_Block *block;
	BF_Block_Init(&block);

	CALL_OR_DIE(BF_Init(LRU));
	CALL_OR_DIE(HT_CreateIndex(FILE_NAME, BUCKETS_NUM));
	CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &fd1));
	CALL_OR_DIE(HT_CloseFile(fd1));
	CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &fd2));
	printf("%d\n", fd2);
	BF_Block_Destroy(&block);
	//CALL_OR_DIE(HT_CloseFile(fd1));
	CALL_OR_DIE(HT_CloseFile(fd2));
	CALL_OR_DIE(BF_Close());
	


}
