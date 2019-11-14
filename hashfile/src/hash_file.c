#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
        BF_PrintError(code);    \
        return HT_ERROR;        \
    }                         \
}

HT_ErrorCode HT_Init() {
    //insert code here
    return HT_OK;
}

#define OFFSET_1 sizeof(char)
#define OFFSET_2 sizeof(char)+sizeof(int)

#define MAX_RECORDS (BF_BLOCK_SIZE-sizeof(int)*2)/sizeof(Record)

/**
 * Our implementation:
 *  - Preallocate a number of 'buckets', for the initial hashing, which we will call our map.
 *  - Any new 'bucket' will go below the map.
 *  When a new record comes, we only have to find its hashing value, and add it to the corrensponding bucket.
 *  Conveniently the hashing value will be the same as the number of the bucket.
 *  If that bucket is full, a new block will be allocated at the end and its number(int) will be added to the previous one, creating something like a linking relation between the two.
 */
HT_ErrorCode HT_CreateIndex(const char *fileName, int buckets) {
    int file_desc;
    CALL_BF(BF_CreateFile(fileName));
    CALL_BF(BF_OpenFile(fileName, &file_desc));
    //printf("%d\n", file_desc);

    BF_Block *block;
    BF_Block_Init (&block);

    char *data;
    CALL_BF(BF_AllocateBlock(file_desc, block));
    data = BF_Block_GetData(block);
    // Set an identifier('*') at the start to indicate that the file is a hashfile.
    // Also set the number of buckets.
    char h = '*';
    memcpy(data, &h, sizeof(char));
    memcpy(data+sizeof(char), &buckets, sizeof(int));
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    
    int zero = 0;
    // Allocate as many blocks as the buckets, which will give us our starting map.
    // Save the number of records at the start of each block (zero at creation).
    // Also save a number which points to the next block, if this block gets filled.
    for (int i = 0; i < buckets; i++) {
        CALL_BF(BF_AllocateBlock(file_desc, block));
        data = BF_Block_GetData(block);
        memcpy(data, &zero, sizeof(int));
        memcpy(data+sizeof(int), &zero, sizeof(int));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
    }
    int blocks_num;
    CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));
    CALL_BF(BF_CloseFile(file_desc));
    
    return HT_OK;
}

/**
 * Opening a file requires validation that the file is indeed a hashfile.
 * The only thing that is needed is a check for the '*' identifier at the start.
 */
HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
    CALL_BF(BF_OpenFile(fileName, indexDesc));

    char *data;
    BF_Block *block;
    BF_Block_Init(&block);

    CALL_BF(BF_GetBlock(*indexDesc, 0, block));
    data = BF_Block_GetData(block);
     
    if (*data != '*'){
            return HT_ERROR;
    }
    CALL_BF(BF_UnpinBlock(block));
    return HT_OK;
}

/**
 * CloseFile just needs to close the file in the indexDesc position.
 */
HT_ErrorCode HT_CloseFile(int indexDesc) {
    CALL_BF(BF_CloseFile(indexDesc));
    return HT_OK;
}

/**
 * The hash function is just a mod, with an addition of 1, to better align the return value with the starting block of the corrensponding bucket in the hashfile.
 */
int hash(int key, int seed){
    return key%seed + 1;
}

/**
 * To InsertEntry in the file all that needs to be done is:
 * 	1. Find its bucket.
 * 	2. Find the position (last record in the bucket).
 * 	3. Insert the record to that position.
 * If there is a need to allocate a new block to the bucket, make sure to chain it with the previous one.
 */
HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
    // Read the number of buckets in the file. The bucket where the record should be inserted is given throught the hash function.
    // In this implementation the hash function uses the number of buckets: hash = id MOD no_of_buckets.

    char *data;
    BF_Block *block;
    BF_Block_Init(&block);
    int buckets_number;

    // The number of buckets is stored in the first block of the hashfile.
    CALL_BF(BF_GetBlock(indexDesc, 0, block));
    data = BF_Block_GetData(block);
    memcpy(&buckets_number, data+sizeof(char), sizeof(int));
    CALL_BF(BF_UnpinBlock(block));

    int bucket_position = hash(record.id, buckets_number);

    // We need to find the last position where we add the record.
    int records;
    int next_block;
    int previous_block = bucket_position;
    CALL_BF(BF_GetBlock(indexDesc, bucket_position, block));
    data = BF_Block_GetData(block);
    memcpy(&records, data, sizeof(int));
    memcpy(&next_block, data+sizeof(int), sizeof(int));

    // Each time we know there exists a next_block and our block is full, we have to follow to that block, until we find the end.
    while(next_block != 0 && records == MAX_RECORDS){
	    CALL_BF(BF_UnpinBlock(block));
	    CALL_BF(BF_GetBlock(indexDesc, next_block, block));
	    data = BF_Block_GetData(block);
	    memcpy(&records, data, sizeof(int));
	    memcpy(&next_block, data+sizeof(int), sizeof(int));
    }
    // When there is no next_block, but our records are maxxed out, we have to allocate a new block and 'link' it.
    if(records == MAX_RECORDS){
	int blocks_number;
    	CALL_BF(BF_GetBlockCounter(indexDesc, &blocks_number));
	memcpy(data+sizeof(int), &blocks_number, sizeof(int));
	BF_Block_SetDirty(block);
	CALL_BF(BF_UnpinBlock(block));

	records = 1;
	next_block = 0;
	CALL_BF(BF_AllocateBlock(indexDesc, block));
	data = BF_Block_GetData(block);
	memcpy(data, &records, sizeof(int));
	memcpy(data+sizeof(int), &next_block, sizeof(int));
    } else {
    	// Otherwise, use this trick to increment the number of records in the block where the record will be added.
	records++;
	memcpy(data, &records, sizeof(int));
    }

    // All that remains, is to place the record in the correct position, meaning underneath all records.
    memcpy(data+sizeof(int)*2+sizeof(Record)*(records-1), &record, sizeof(Record));
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    /*
    char *myBlockData;
    int bucketSum;
    BF_Block *myBlock, *newBlock;
    BF_Block_Init(&myBlock);
    BF_Block_Init(&newBlock);
    //int max_records = (BF_BLOCK_SIZE-sizeof(int)*2)/sizeof(Record);

    // The number of buckets is stored in the first block of the hashfile.
    CALL_BF(BF_GetBlock(indexDesc, 0, myBlock));
    myBlockData = BF_Block_GetData(myBlock);
    memcpy(&bucketSum, myBlockData+sizeof(char), sizeof(int));
    CALL_BF(BF_UnpinBlock(myBlock));

    int bucketPos = hash(record.id, bucketSum);
    printf("bucketPos%d\n", bucketPos);
    //printf("%d\n", bucket_position);
    
    int recordsInBlock;
    int connectedBlock;
    int previousBlock = bucketPos;
    int blockNum;
    CALL_BF(BF_GetBlock(indexDesc, bucketPos, myBlock));
    myBlockData = BF_Block_GetData(myBlock);
    //memcpy(&recordsInBlock, myBlockData, sizeof(int));
    //memcpy(&connectedBlock, myBlockData+sizeof(int), sizeof(int));

    int recordsInBlockInit = 0;
    int connectedBlockInit = 0;
    //printf("spera\n");

    while (1)
    {
        memcpy(&recordsInBlock, myBlockData, sizeof(int));
        memcpy(&connectedBlock, myBlockData+sizeof(int), sizeof(int));

        if(recordsInBlock == MAX_RECORDS)
        {
            if(connectedBlock == 0) // Create new Block
            {
                printf("mpike1\n");
                // Creation of new block, link to it, setdirty, unpin last block
                // make myBlockData show the connected block
                // continue;
                //BF_Block_Init(&newBlock);
                CALL_BF(BF_AllocateBlock(indexDesc, newBlock));
                CALL_BF(BF_GetBlockCounter(indexDesc, &blockNum));
                printf("blockNum%d\n", blockNum);
                memcpy(myBlockData+sizeof(int), &blockNum, sizeof(int));
                BF_Block_SetDirty(myBlock);
                CALL_BF(BF_UnpinBlock(myBlock));
                myBlockData = BF_Block_GetData(newBlock);
                myBlock = newBlock;
                memcpy(myBlockData, &recordsInBlockInit, sizeof(int));
                memcpy(myBlockData+sizeof(int), &connectedBlockInit, sizeof(int));
                continue;
            }
            else
            {
                printf("mpike2\n");
                printf("connected%d\n", connectedBlock);
                // make myBlockData show the connected block, unpin previous block
                // continue; (in this case newBlock stands for nextBlock)
                CALL_BF(BF_GetBlock(indexDesc, connectedBlock, newBlock));
                myBlockData = BF_Block_GetData(newBlock);
                CALL_BF(BF_UnpinBlock(myBlock));
                myBlock = newBlock;
                continue;
            }
        }
        else
        {
            printf("mpike3\n");
            // memcpy the new record
            // up 1 the recordsInBlock
            // setdirty and unpin
            // break;
            memcpy(myBlockData+sizeof(int)*2+recordsInBlock*sizeof(Record), &record, sizeof(Record));
            recordsInBlock++;
            printf("%d\n", recordsInBlock);
            printf("%d\n", record.id);
            memcpy(myBlockData, &recordsInBlock, sizeof(int));
            BF_Block_SetDirty(myBlock);
            CALL_BF(BF_UnpinBlock(myBlock));
            break;
        }
    }*/


    /*int records;
    int next_block;
    int previous_block = bucket_position;
    CALL_BF(BF_GetBlock(indexDesc, bucket_position, block));
    data = BF_Block_GetData(block);
    memcpy(&records, data, sizeof(int));
    memcpy(&next_block, data+sizeof(int), sizeof(int));

    while(next_block != 0 && records != 0){
        previous_block = next_block;
        CALL_BF(BF_UnpinBlock(block));
            CALL_BF(BF_GetBlock(indexDesc, next_block, block));
        data = BF_Block_GetData(block);
        memcpy(&records, data, sizeof(int));
        memcpy(&next_block, data+sizeof(int), sizeof(int));
    }
    if( records == 0) {
            next_block = previous_block;
        CALL_BF(BF_UnpinBlock(block));
        CALL_BF(BF_GetBlock(indexDesc, next_block, block));
        data = BF_Block_GetData(block);
        memcpy(&records, data, sizeof(int));
        memcpy(&next_block, data+sizeof(int), sizeof(int));
    }

    if( records >= MAX_RECORDS ){
        int blocks_num;
            CALL_BF(BF_GetBlockCounter(indexDesc, &blocks_num));
        memcpy(data+sizeof(int), &blocks_num, sizeof(int));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        records = 1;
        next_block = 0;
        CALL_BF(BF_AllocateBlock(indexDesc, block));
        data = BF_Block_GetData(block);
        memcpy(data, &records, sizeof(int));
        memcpy(data+sizeof(int), &next_block, sizeof(int));
        memcpy(data+sizeof(int)*2, &record, sizeof(Record));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
    } else {
            memcpy(data+sizeof(int)*2+records*sizeof(Record), &record, sizeof(Record));
        records++;
        memcpy(data, &records, sizeof(int));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
    }*/

    return HT_OK;
}

/**
 * To print all entries given an id, all we have to do is iterate over the chain of blocks in the corrensponding bucket, given through the hash function.
 * If the id is NULL then we have to print all entries, meaning iterate over all blocks and print all their records.
 */
HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    if ( id == NULL ){
        // Print all entries in the hashfile.
        char *data;
        BF_Block *block;
        BF_Block_Init(&block);
        int blocks_num;
        CALL_BF(BF_GetBlockCounter(indexDesc, &blocks_num));

        for (int i = 1; i <= blocks_num-1; i++){
                CALL_BF(BF_GetBlock(indexDesc, i, block));
                data = BF_Block_GetData(block);
                int records_num;
                memcpy(&records_num, data, sizeof(int));

                for (int j = 1; j <= records_num; j++) {
                        Record *rec;
                        rec = (Record *)malloc(sizeof(Record));
                        memcpy(rec, data+sizeof(int)*2+(j-1)*sizeof(Record), sizeof(Record));
                                printf("(%d, %s, %s, %s)\n", rec->id, rec->name, rec->surname, rec->city);
                        free(rec);
                }
                CALL_BF(BF_UnpinBlock(block));
        }
    } else {
        char *data;
        BF_Block *block;
        BF_Block_Init(&block);
        int no_of_buckets;

        CALL_BF(BF_GetBlock(indexDesc, 0, block));
        data = BF_Block_GetData(block);
        memcpy(&no_of_buckets, data+sizeof(char), sizeof(int));
        int bucket_position = hash(*id, no_of_buckets);
        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(indexDesc, bucket_position, block));
        data = BF_Block_GetData(block);
        int flag = 0;
        int records;
        int next_block;
        Record *record;
        record = (Record*)malloc(sizeof(Record));
        memcpy(&records, data, sizeof(int));
        memcpy(&next_block, data+sizeof(int), sizeof(int));
        while( flag == 0 ){
                for( int i = 0; i < records; i++ ){
                        memcpy(record, data+sizeof(int)*2+sizeof(Record)*i, sizeof(Record));
                        if( record->id == *id ){
                                printf("(%d, %s, %s, %s)\n", record->id, record->name, record->surname, record->city);
                                flag = 1;
                        }
                }
                if (next_block != 0 ) {
                        CALL_BF(BF_UnpinBlock(block));
                        CALL_BF(BF_GetBlock(indexDesc, next_block, block));
                        data = BF_Block_GetData(block);
                        memcpy(&records, data, sizeof(int));
                        memcpy(&next_block, data+sizeof(int), sizeof(int));
                } else {
                        printf("Couldn't find the key.\n");
                        free(record);
                        CALL_BF(BF_UnpinBlock(block));
                        return HT_OK;
                }
        }
        free(record);
        CALL_BF(BF_UnpinBlock(block));
    }

    return HT_OK;
}


/**
 * DeleteEntry works like this:
 *  - First we find the record position of the id we want to delete.
 *  -- If there is only one record in the block, meaning the last record of the bucket, just decrement the record number in the block.
 *  -- If there are more records in the bucket, then we find the last record, put it in the variable replacer_record, delete it and place it in the position of the record that we want to delete.
 *  If we don't find the id then we return HT_ERROR.
 */
HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
    char *data;
    BF_Block *block;
    BF_Block_Init(&block);

    CALL_BF(BF_GetBlock(indexDesc, 0, block));
    data = BF_Block_GetData(block);
    int no_of_buckets;
    memcpy(&no_of_buckets, data+sizeof(char), sizeof(int));
    int bucket_position = hash(id, no_of_buckets);
    CALL_BF(BF_UnpinBlock(block));

    CALL_BF(BF_GetBlock(indexDesc, bucket_position, block));
    data = BF_Block_GetData(block);
    int records;
    int next_block;
    memcpy(&records, data, sizeof(int));
    memcpy(&next_block, data+sizeof(int), sizeof(int));

    if(records == 0){
            printf("Error: bucket has 0 records.\n");
        return HT_ERROR;
    }

    Record *record = (Record*)malloc(sizeof(Record));
    Record *replacer_record = (Record*)malloc(sizeof(Record));

    int flag = 0;
    while( flag == 0 ){
    	for(int i = 0; i < records; i++){
		memcpy(record, data+sizeof(int)*2+sizeof(Record)*i, sizeof(Record));
		if (record->id == id) {
			// We found the record to delete.
			if (records == 1){
				// If we only have one record, meaning there is no other to replace it, simply decrease the number of records in the block.
				records--;
				memcpy(data, &records, sizeof(int));
			} else {
				// Else, we need to find the last record in the bucket, remove that and place it in the record's position.
				Record *replacer_record = (Record*)malloc(sizeof(Record));

				char *data2;
				BF_Block *block2;
				BF_Block_Init(&block2);

				CALL_BF(BF_GetBlock(indexDesc, bucket_position, block2));
				data2 = BF_Block_GetData(block2);
				int this_records;
				int this_block;
				int previous_block = bucket_position;
				memcpy(&this_records, data2, sizeof(int));
				memcpy(&this_block, data2+sizeof(int), sizeof(int));
				
				while(this_block != 0 && this_records == MAX_RECORDS) {
					previous_block = this_block;
					CALL_BF(BF_UnpinBlock(block2));
					CALL_BF(BF_GetBlock(indexDesc, this_block, block2));
					data2 = BF_Block_GetData(block2);
					memcpy(&this_records, data2, sizeof(int));
					memcpy(&this_block, data2+sizeof(int), sizeof(int));
				}
				if(this_records == 0){
					CALL_BF(BF_UnpinBlock(block2));
					CALL_BF(BF_GetBlock(indexDesc, previous_block, block2));
					data2 = BF_Block_GetData(block2);
					memcpy(&this_records, data2, sizeof(int));
					memcpy(&this_block, data2, sizeof(int));
				}

				memcpy(replacer_record, data2+sizeof(int)*2+sizeof(Record)*(this_records-1), sizeof(Record));
				this_records--;
				memcpy(data2, &this_records, sizeof(int));
				BF_Block_SetDirty(block2);
				CALL_BF(BF_UnpinBlock(block2));

				// We now have the last record in replacer_record.
				memcpy(data+sizeof(int)*2+sizeof(Record)*i, replacer_record, sizeof(Record));
			}
			// In both cases, we altered the block.
			BF_Block_SetDirty(block);
			CALL_BF(BF_UnpinBlock(block));
			flag = 1;
		}
		if(flag == 1){
			// Job is done.
			free(record);
			free(replacer_record);
			return HT_OK;
			break;
		}
	}
	if(next_block != 0){
		CALL_BF(BF_UnpinBlock(block));
		CALL_BF(BF_GetBlock(indexDesc, next_block, block));
		data = BF_Block_GetData(block);
		memcpy(&records, data, sizeof(int));
		memcpy(&next_block, data+sizeof(int), sizeof(int));
	} else {
		printf("Error: The id %d, could not be found\n", id);
		return HT_ERROR;
	}
	
    }
    return HT_OK;
}
