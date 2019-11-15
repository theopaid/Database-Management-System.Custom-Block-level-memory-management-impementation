hp:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

test1:
	@echo " Compile test_1 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/test_1.c ./src/hash_file.c -lbf -o ./build/test1 -O2

hp1:
	@echo " Compile hp_test1 ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/hp_mainTest1.c ./src/hash_file.c -lbf -o ./build/hp1 -O2
	
diff_buckets:
	@echo " Compile diff_buckets ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/diff_buckets.c ./src/hash_file.c -lbf -o ./build/runner -O2

same_file:
	@echo " Compile same_file.c ....";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/same_file.c ./src/hash_file.c -lbf -o ./build/runner -O2
	
randRecords_delAll.c:
	@echo " Compile randRecords_delAll.c ..."
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/randRecords_delAll.c ./src/hash_file.c -lbf -o ./build/runner -O1

clear:
	@echo " Cleaning data files ...";
	rm data*.db;
	
cdata:
	@echo " Cleaning data.db ...";
	rm data.db;
