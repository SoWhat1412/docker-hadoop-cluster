:begin
MATCH (n) DETACH DELETE n
;
:commit

:begin
DROP INDEX ON :Person(number)
;
:commit

:begin
UNWIND range(1,10) as number
CREATE (p:Person {number:number}) WITH collect(p) as people
UNWIND people as p1
UNWIND range(1,2) as friend
WITH p1, people[(p1.number + friend) % size(people)] as p2
CREATE (p1)-[:KNOWS {years: abs(p2.number - p1.number)}]->(p2)
;
:commit

:begin
CREATE INDEX ON :Person(number)
;
:commit
