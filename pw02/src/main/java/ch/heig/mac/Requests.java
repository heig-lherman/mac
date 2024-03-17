package ch.heig.mac;

import java.util.List;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = """
                CALL db.labels
                """;

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickVisit:VISITS]->(:Place)<-[healthyVisit:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickVisit.starttime > sick.confirmedtime
                  AND healthyVisit.starttime > healthy.confirmedtime
                  AND healthyVisit.starttime > sickVisit.starttime
                RETURN DISTINCT sick.name AS sickName
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickVisit:VISITS]->(:Place)<-[healthyVisit:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickVisit.starttime > sick.confirmedtime
                  AND healthyVisit.starttime > healthy.confirmedtime
                  AND healthyVisit.starttime > sickVisit.starttime
                RETURN sick.name AS sickName,
                       COUNT(healthy) AS nbHealthy
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[v:VISITS]->(p:Place)
                WHERE v.starttime > sick.confirmedtime
                WITH sick, COUNT(DISTINCT p) AS nbPlaces
                WHERE nbPlaces > 10
                RETURN sick.name AS sickName, nbPlaces
                ORDER BY nbPlaces DESC
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})
                WHERE NOT EXISTS {
                    (sick)-[v:VISITS]->(p:Place {type:'Bar'})
                    WHERE v.starttime > sick.confirmedtime
                }
                RETURN sick.name AS sickName
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> peopleToInform() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickVisit:VISITS]->(:Place)<-[healthyVisit:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickVisit.starttime > sick.confirmedtime
                  AND healthyVisit.starttime > healthy.confirmedtime
                WITH sick, healthy,
                     duration.inSeconds(
                         apoc.coll.max([sickVisit.starttime, healthyVisit.starttime]),
                         apoc.coll.min([sickVisit.endtime, healthyVisit.endtime])
                     ) AS overlap
                WHERE datetime() + duration({hours:2}) <= datetime() + overlap
                RETURN sick.name AS sickName,
                       COLLECT(healthy.name) AS peopleToInform
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> setHighRisk() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickVisit:VISITS]->(:Place)<-[healthyVisit:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickVisit.starttime > sick.confirmedtime
                  AND healthyVisit.starttime > healthy.confirmedtime
                WITH sick, healthy,
                     duration.inSeconds(
                         apoc.coll.max([sickVisit.starttime, healthyVisit.starttime]),
                         apoc.coll.min([sickVisit.endtime, healthyVisit.endtime])
                     ) AS overlap
                WHERE datetime() + duration({hours:2}) <= datetime() + overlap
                SET healthy.risk = 'high'
                RETURN DISTINCT healthy.id AS highRiskId,
                                healthy.name AS highRiskName
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.list();
        }
    }

    public List<Record> healthyCompanionsOf(String name) {
        var query = """
                MATCH (p:Person {name: $name})-[v:VISITS*6]-(c:Person {healthstatus:'Healthy'})
                WHERE c <> p
                RETURN DISTINCT c.name AS healthyName
                """;

        try (var session = driver.session()) {
            var result = session.run(query, Values.parameters("name", name));
            return result.list();
        }
    }

    public Record topSickSite() {
        var query = """
                MATCH (sick:Person {healthstatus:'Sick'})-[v:VISITS]->(p:Place)
                WHERE v.starttime > sick.confirmedtime
                RETURN p.type AS placeType,
                       COUNT(v) AS nbOfSickVisits
                ORDER BY nbOfSickVisits DESC
                LIMIT 1
                """;

        try (var session = driver.session()) {
            var result = session.run(query);
            return result.next();
        }
    }

    public List<Record> sickFrom(List<String> names) {
        var query = """
                MATCH (p:Person {healthstatus:'Sick'})
                WHERE p.name IN $names
                RETURN p.name AS sickName
                """;

        try (var session = driver.session()) {
            var result = session.run(query, Values.parameters("names", names));
            return result.list();
        }
    }
}
