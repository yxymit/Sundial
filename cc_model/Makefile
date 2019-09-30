all: run

include=scheduler.h opt_scheduler.h nowait_scheduler.h limitdepth_scheduler.h occ_scheduler.h \
	waitdie_scheduler.h sortwait_scheduler.h dldetect_scheduler.h lockviolation_scheduler.h \
	woundwait_scheduler.h

run: main.cc $(include)
	g++ -g -ggdb -O0 -o run main.cc
clean:
	rm run
