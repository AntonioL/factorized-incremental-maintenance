# DSI Exam

This is my exam of Database Systems Implementation for the MSc in Computer Science at
the University of Oxford.
You can find the exam sheet in the file "DSI_2016_Exam_Sheet.pdf".

We had to design and implement algorithms for the task of incremental maintenance of
materialized views.

## My solution

I designed and implemented novel algorithms for incremental maintenance of materialized views,
in the process I exploited the framework laid out by Factorized Representations [1], and at the same
time I borrowed some ideas from Information Retrieval (the idea of inverted indexes).

You can find the report in "DSI_Exam.pdf", the structure follows the order in which the questions
were posed in the exam sheet.

For instructions on running my code look at the "DSI_Documentation.pdf" file.

## Contributions

* I propose two representations for materialized views:
	* By means of a factorization tree if the conjunctive query is acyclic.
	* Second, I decompose the view in a cyclic part, represented through Inverted Indexes,
	  and an acyclic part, represented via a factorization tree.
* I discuss (briefly) the problem of finding a suitable factorization tree layout and I propose a solution.
* I exploit structural properties of the query to estabilish the best way to represent its result
  (ie. if the query is cyclic or acyclic).
* I exploit the "lightweight join" trick as it was hinted in the exam.
* I provide a package of algorithms for updating the materialized views upon insertion and deletion
  of a record. In the process I also analyse thoroughly the asymptotic complexity of my procedures.

#References

[1] Olteanu, Dan, and Jakub Závodný. "Size bounds for factorised representations of query results." ACM Transactions on Database Systems (TODS) 40.1 (2015): 2.