.SUFFIXES:
.PHONY: all

all: hisser/build.ts

hisser/build.ts: hisser/compile.ts
	python setup.py build_ext --inplace
	touch $@

hisser/compile.ts: hisser/pack.c hisser/jsonpoints.cpp
	touch $@

%.cpp: %.pyxx
	cython -3 --cplus -a $<

%.c: %.pyx
	cython -3 -a $<
