#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"


#define RECORDS_NUM 1700 // you can change it if you want
//#define BUCKETS_NUM 13  // you can change it if you want
#define FILE_NAME "data.db"
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
    char *fileName = "data.db";
    int maxBuckets;
    maxBuckets = rand() % 20 + 1;
    srand(time(NULL)); //randomize maximum buckets
    CALL_OR_DIE(HT_CreateIndex(fileName, maxBuckets));

    for(int i = 0; i < MAX_OPEN_FILES; i++) {
 
        printf("%s\n", fileName);
        CALL_OR_DIE(HT_OpenIndex(fileName, &indexDesc));

        if(i == 0) {

            Record record;
            int r;
            printf("Insert Entries\n");
            for (int id = 0; id < RECORDS_NUM; ++id) {
                record.id = id;
                r = rand() % 12;
                memcpy(record.name, names[r], strlen(names[r]) + 1);
                r = rand() % 12;
                memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
                r = rand() % 10;
                memcpy(record.city, cities[r], strlen(cities[r]) + 1);

                CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
            }
        }

        printf("RUN PrintAllEntries\n");
	    printf("|||||||||||||||||||||||||||||||||  FILE %d  |||||||||||||||||||||||||||||||||\n", indexDesc);
        int id = rand() % RECORDS_NUM;
        CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

        printf("Delete Entry with id = %d\n" ,id);
        CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));
        printf("Print Entry with id = %d\n", id); 
        CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id)); // must print something like : Entry doesn't exist or nothing at all
    }

    //Let's close one file and then add another
    CALL_OR_DIE(HT_CloseFile(2));

    printf("%s\n", fileName);
    
    CALL_OR_DIE(HT_OpenIndex(fileName, &indexDesc));

    printf("RUN PrintAllEntries\n");
    printf("||||||||||||||||||||||||||||||||||||||  FILE: %d ||||||||||||||||||||||||||||||\n", indexDesc);
    int id = rand() % RECORDS_NUM;
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

    printf("Delete Entry with id = %d\n" ,id);
    CALL_OR_DIE(HT_DeleteEntry(indexDesc, id));
    printf("Print Entry with id = %d\n", id); 
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id)); // must print something like : Entry doesn't exist or nothing at all

    // Closing all 5 files created
    for(int i = 0; i < MAX_OPEN_FILES; i++) {
        CALL_OR_DIE(HT_CloseFile(i));
    }
    BF_Close();
    
}
