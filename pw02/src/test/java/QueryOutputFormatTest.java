import ch.heig.mac.Main;
import ch.heig.mac.Requests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class QueryOutputFormatTest {

    private Driver driver;
    private Requests requests;

    @BeforeEach
    public void setUp() {
        driver = Main.openConnection();
        requests = new Requests(driver);
    }

    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void testGetDbLabelsQuery() {
        assertThat(requests.getDbLabels())
                .hasSameElementsAs(List.of("_Bloom_Perspective_", "Person", "Place", "Visit", "Region", "Country", "Continent"));
    }

    @Test
    public void testPossibleSpreadersQuery() {
        var res = requests.possibleSpreaders().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
    }

    @Test
    public void testPossibleSpreadCountsQuery() {
        var res = requests.possibleSpreadCounts().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName", "nbHealthy"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
        assertThatCode(() -> res.get("nbHealthy").asInt())
                .doesNotThrowAnyException();
    }

    @Test
    public void testCarelessPeopleQuery() {
        var res = requests.carelessPeople().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName", "nbPlaces"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
        assertThatCode(() -> res.get("nbPlaces").asInt())
                .doesNotThrowAnyException();
    }

    @Test
    public void testSociallyCarefulQuery() {
        var res = requests.sociallyCareful().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
    }

    @Test
    public void testPeopleToInformQuery() {
        var res = requests.peopleToInform().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName", "peopleToInform"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
        assertThatCode(() -> res.get("peopleToInform").asList(Value::asString))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSetHighRiskQuery() {
        var res = requests.setHighRisk().get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("highRiskId", "highRiskName"));

        // Check type
        assertThatCode(() -> res.get("highRiskId").asString())
                .doesNotThrowAnyException();
        assertThatCode(() -> res.get("highRiskName").asString())
                .doesNotThrowAnyException();
    }

    @Test
    public void testHealthyCompanionsOfQuery() {
        var res = requests.healthyCompanionsOf("Rocco Mendez").get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("healthyName"));

        // Check type
        assertThatCode(() -> res.get("healthyName").asString())
                .doesNotThrowAnyException();
    }

    @Test
    public void testTopSickSiteQuery() {
        var res = requests.topSickSite();
        assertThat(res.keys())
                .hasSameElementsAs(List.of("placeType", "nbOfSickVisits"));

        // Check type
        assertThatCode(() -> res.get("placeType").asString())
                .doesNotThrowAnyException();
        assertThatCode(() -> res.get("nbOfSickVisits").asInt())
                .doesNotThrowAnyException();
    }

    @Test
    public void testSickFromQuery() {
        var res = requests.sickFrom(List.of("Landyn Greer", "Saniyah Fuller", "Baylee Leblanc")).get(0);
        assertThat(res.keys())
                .hasSameElementsAs(List.of("sickName"));

        // Check type
        assertThatCode(() -> res.get("sickName").asString())
                .doesNotThrowAnyException();
    }
}
