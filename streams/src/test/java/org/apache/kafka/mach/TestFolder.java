package org.apache.kafka.mach;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.temporal.ChronoField.*;


/**
 * A rule for creating a test output folder which is based on the timestamp when the rule is run along with the test name.
 * It does not delete the folder after the test is finished so that we can inspect the output manually.
 */
public class TestFolder implements TestRule
{
    /**
     * The logger to use for any debug info.
     */
    private Logger logger = LoggerFactory.getLogger(TestFolder.class);

    /**
     * The root folder to create sub folders under for each test being run.
     */
    private final Path rootFolder;

    /**
     * The name of the class being run.
     */
    private String className;

    /**
     * The name of the test being run.
     */
    private String testName;

    /**
     * The actual folder for the test.
     */
    private Path testFolder;

    /**
     * The timestamp of when this rule was created.
     * This is used to timestamp the output folders.
     */
    private static LocalDateTime timestamp = LocalDateTime.now();

    /**
     * Creates a new folder for the test that is running.
     * The root folder is './test-output'.
     * The timestamp, class name and test name is used as the root folder.
     */
    public TestFolder()
    {
        this("./../test-output");
    }

    /**
     * Creates a new folder for the test that is running.
     * It uses the given root path for the test output folder.
     * The timestamp, class name and test name is used as the root folder.
     * @param rootFolder The root folder to use to derive test folders underneath. They will be derived from the test name and timestamp.
     */
    public TestFolder(String rootFolder)
    {
        this.rootFolder = Paths.get(rootFolder);
    }

    @Override public Statement apply(Statement statement, Description description)
    {
        // Save the details of the test that is running:
        className = description.getTestClass().getSimpleName();
        testName = description.getMethodName();

        // Prepare the folder:
        testFolder = this.deriveTestFolder(rootFolder, timestamp, className, testName);

        // Create the test folder:
        try
        {
            Files.createDirectories(testFolder);
        }
        catch (IOException e)
        {
            logger.error("Error creating test folder.", e);
        }

        // Allow the original statement to be processed after this one:
        return statement;
    }

    /**
     * Derives the test folder for the given test.
     * @param rootFolder The root folder to descend from.
     * @param timestamp The timestamp of when the tests started running.
     * @param className The name of the class that is running.
     * @param testName The name of the test that is running.
     * @return The path to the test folder.
     */
    protected Path deriveTestFolder(Path rootFolder, LocalDateTime timestamp, String className, String testName)
    {
        DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                .appendValue(YEAR, 4)
                .appendLiteral('-')
                .appendValue(MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(DAY_OF_MONTH, 2)
                .appendLiteral('-')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral('-')
                .appendValue(MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral('-')
                .appendValue(SECOND_OF_MINUTE, 2)
//            .optionalStart()
//            .appendFraction(NANO_OF_SECOND, 0, 9, true)
                .toFormatter();
        return rootFolder.resolve(timestamp.format(dateTimeFormatter)).resolve(className).resolve(testName);
    }

    /**
     * Creates a new folder under the test folder.
     * @param folderName The name of the folder to create.
     * @return The path to the newly created folder.
     */
    public Path newFolder(String folderName) throws IOException
    {
        // Create the new folder:
        Path folder = this.testFolder.resolve(folderName);

        // Make sure the folder exists:
        Files.createDirectories(folder);

        return folder;
    }

    /**
     * Creates a new folder hierarchy under the test folder.
     * @param folderNames The names of the sub folders to create under the test folder.
     * @return The path to the leaf folder that was created.
     */
    public Path newFolders(String... folderNames) throws IOException
    {
        // Start at the test folder:
        Path folder = this.testFolder;

        // Walk each folder that was given:
        for (String folderName : folderNames)
        {
            // Walk the folder:
            folder = folder.resolve(folderName);
        }
        // Now we have walked all the folders:

        // Make sure the folder exists:
        Files.createDirectories(folder);

        return folder;
    }

    /**
     * Gets the root folder to create sub folders under for each test being run.
     * @return The root folder to create sub folders under for each test being run.
     */
    public Path getRootFolder()
    {
        return rootFolder;
    }

    /**
     * Gets the name of the class being run.
     * @return The name of the class being run.
     */
    public String getClassName()
    {
        return className;
    }

    /**
     * Gets the name of the test being run.
     * @return The name of the test being run.
     */
    public String getTestName()
    {
        return testName;
    }

    /**
     * Gets the actual folder for the test.
     * @return The actual folder for the test.
     */
    public Path getTestFolder()
    {
        return testFolder;
    }

    /**
     * Gets the timestamp of when this rule was created.
     * @return The timestamp of when this rule was created.
     */
    public LocalDateTime getTimestamp()
    {
        return timestamp;
    }
}
