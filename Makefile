.PHONY = pytest clean

pytest: 
	 python3 -m pytest

clean:
		(rm -rf __pycache__ ; rm -rf */__pycache__ ; rm -rf *.pyc ; rm -rf */*.pyc )
