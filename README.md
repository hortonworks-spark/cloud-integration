# spark-cloud-examples

This does the packaging/integration tests for Spark and cloud against AWS, Azure and openstack.

These are basic tests of the core functionality of I/O, streaming, using spark as a destination of work.
As well as running as unit tests, they have CLI entry points which can be used for scalable functional testing.

(Though some of the code there is now starting to work with some private repos that I've created off public datasets; ping
for help setting up your own copies).

Passing these tests does not mean that you can safely use the object stores, particularly S3, as a destination of work.
It's not consistent, and data may be lost.
