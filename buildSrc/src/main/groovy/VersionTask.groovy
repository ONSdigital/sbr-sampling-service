import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

public class VersionTask extends DefaultTask{
    private String DELIMITER = '.'
    private String prevMajor
    private String prevMinor
    int major
    int minor

    def File versionFile

    def setVersion(int major, int minor) {
        doFirst {
            this.major = major
            this.minor = minor
            versionFile.text = "version=${this.major}.${this.minor} + delimBuildNumber"
        }
    }

    /**
     * @throws {ArrayIndexOutOfBoundsException} - list length determines there is no head or last element
     * @throws {NumberFormatException} - major or minor values cannot be converted to a string i.e. not a valid whole number string
     *
     * @note {ArrayIndexOutOfBoundsException} - does not cover when version scheme is altered such as %d.%d.%d. In such case,
     *  the last and first are only taken and subsequently stored
     */
    def fromString(String rawText, String delimiter = DELIMITER) throws ArrayIndexOutOfBoundsException, NumberFormatException {
        prevMajor = rawText.tokenize(delimiter).head()
        prevMinor = rawText.tokenize(delimiter).get(1)
        new Tuple(prevMajor as int, prevMinor as int)
    }

    String toString() {
        return "Bumped version from [$prevMajor.$prevMinor] to [$major.$minor]"
    }

    @TaskAction
    void bump() {
        print this
    }
}
