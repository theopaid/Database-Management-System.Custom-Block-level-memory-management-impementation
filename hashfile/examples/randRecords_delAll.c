#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

//#define RECORDS_NUM 1700 // you can change it if you want
//#define BUCKETS_NUM 13  // you can change it if you want
//#define FILE_NAME "data.db"
#define MAX_OPEN_FILES 20

const char* names[] = {
    "Yannis",
    "Christofos",
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
    "Nikolopoulos",
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
    "Hong Kong",
    "Munich",
    "Miami"
};

#define CALL_OR_DIE(call)     \
    {                           \
        HT_ErrorCode code = call; \
        if (code != HT_OK) {      \
            printf("Error\n");      \
            exit(code);             \
        }                         \
    }


int main() {
    BF_Init(LRU);

    CALL_OR_DIE(HT_Init());

    int indexDesc;
    char fileName[10];
    int maxBuckets;
    int recordsNum;
    int fileNameCounter = 0;
    srand(time(NULL)); //randomize maximum buckets

    for(int i = 1; i <= MAX_OPEN_FILES; i++) {
        maxBuckets = rand() % 20 + 5;
        recordsNum = rand() % 2500 + 100;
        fileNameCounter++;
        snprintf(fileName, 10, "data%d.db", fileNameCounter);
        CALL_OR_DIE(HT_CreateIndex(fileName, maxBuckets));
        CALL_OR_DIE(HT_OpenIndex(fileName, &indexDesc));

        Record record;
        int r;
        printf("Insert Entries\n");
        for (int id = 0; id < recordsNum; ++id) {
            record.id = id;
            r = rand() % 12;
            memcpy(record.name, names[r], strlen(names[r]) + 1);
            r = rand() % 12;
            memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
            r = rand() % 10;
            memcpy(record.city, cities[r], strlen(cities[r]) + 1);

            CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
        }

        printf("RUN PrintAllEntries\n");
	    printf("|||||||||||||||||||||||||||||||||  FILE %d  |||||||||||||||||||||||||||||||||\n", fileNameCounter);
        int id = rand() % recordsNum;
        //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
        CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

	if ( indexDesc != 19 ){
   		printf("Delete Entry with id = %d\n" ,id);
        	CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));
        	printf("Print Entry with id = %d\n", id); 
        	CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id)); // must print something like : Entry doesn't exist or nothing at all
	}

    }

    // Let's delete all last file's records
    for(int i = 0; i < recordsNum; i++) {
        CALL_OR_DIE(HT_DeleteEntry(19, i));
        printf("Print Entry with id = %d /%d\n", i, recordsNum-1);
        CALL_OR_DIE(HT_PrintAllEntries(19, &i));
    }
    CALL_OR_DIE(BF_CloseFile(indexDesc));
    BF_Close();
}
