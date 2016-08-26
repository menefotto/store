This package implements a simple graph with two back ends choices, an in memory
backend and a persistent backends using boltdb for persistence.
To implement another backend using a database of your choice, you have to implement
the DB interface.

Disclaimer:
- Do not use this software in a production environment, it hasn't been tested or
  is ready for production this is a very early release. It hasn't been finished
  yet even if it works as it should.
