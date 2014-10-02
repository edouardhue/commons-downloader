package commonsdl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.EventLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.StructuredDataMessage;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import commonsdl.Download.Mode;

public final class CommonsDownloader {
	
	private static final Logger LOGGER = LogManager.getLogger(CommonsDownloader.class);
	
	private final Download download;
	
	private final CloseableHttpAsyncClient client;
	
	private final URI commons;
	
	private CountDownLatch latch; 
	
	public CommonsDownloader(final Download download) throws URISyntaxException {
		this.download = download;
		this.client = HttpAsyncClients.createDefault();
		this.commons = new URIBuilder("https://commons.wikimedia.org/w/index.php").addParameter("title", "Special:FilePath").build();
	}

	public void download() throws IOException {
		this.client.start();
		try {
			final int lines = (int) Files.lines(download.getFile(), download.getCharset()).count();
			LOGGER.info("Will download {} files", lines);
			this.latch = new CountDownLatch(lines);
			Files.lines(download.getFile(), download.getCharset())
				.parallel()
				.map(s -> s.substring(0, s.lastIndexOf(',')))
				.forEach(s -> this.download(s, download.getDestination()));
		} finally {
			try {
				LOGGER.info("Waiting for downloads to complete");
				latch.await();
			} catch (final InterruptedException e) {
				Thread.interrupted();
			}
			LOGGER.info("All done");
			this.client.close();
		}
	}
	
	private void download(final String fileName, final Path destination) {
		final StructuredDataMessage event = new StructuredDataMessage(Integer.toHexString(fileName.hashCode()) , null, "download");
		final Path destinationFile = Paths.get(destination.toString(), fileName);
		event.put("destinationFile", destinationFile.toAbsolutePath().toString());
		if (download.getMode() == Mode.RESTART || !destinationFile.toFile().exists()) {
			try {
				final URI uri = new URIBuilder(commons).addParameter("file", fileName).build();
				event.put("uri", uri.toString());
				final HttpGet request = new HttpGet(uri);
				client.execute(request, new DownloaderCallback(fileName, destination, event));
			} catch (final URISyntaxException e) {
				event.put("status", "error");
				EventLogger.logEvent(event, Level.WARN);
				LOGGER.warn("Could not build URI for {}.", fileName, e);
				latch.countDown();
			}
		} else {
			event.put("status", "skipped");
			EventLogger.logEvent(event, Level.DEBUG);
			latch.countDown();
		}
	}

	public static void main(String[] args) {
		final Download download = new Download();
		final CmdLineParser parser = new CmdLineParser(download);
		try {
			parser.parseArgument(args);
			final CommonsDownloader downloader = new CommonsDownloader(download);
			downloader.download();
		} catch (final CmdLineException e) {
			parser.printUsage(System.out);
		} catch (final URISyntaxException | IOException e) {
			System.err.printf("Could not open file: %1$s.", e.getMessage());
		}
	}
	
	private final class DownloaderCallback implements FutureCallback<HttpResponse> {
		
		private final String fileName;
		
		private final Path destination;
		
		private final StructuredDataMessage event;
		
		public DownloaderCallback(final String fileName, final Path destination, final StructuredDataMessage event) {
			this.fileName = fileName;
			this.destination = destination;
			this.event = event;
		}
		
		@Override
		public void failed(final Exception e) {
			latch.countDown();
			event.put("status", "error");
			EventLogger.logEvent(event, Level.WARN);
			LOGGER.warn("Donwload of {} failed.", fileName, e);
		}

		@Override
		public void completed(final HttpResponse response) {
			final Path destinationFile = Paths.get(destination.toString(), fileName);
			try (final BufferedOutputStream os = new BufferedOutputStream(Files.newOutputStream(destinationFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
				response.getEntity().writeTo(os);
				event.put("status", "success");
				EventLogger.logEvent(event, Level.INFO);
			} catch (final IOException e) {
				event.put("status", "error");
				EventLogger.logEvent(event, Level.WARN);
				LOGGER.warn("Failed to save {}.", fileName, e);
			} finally {
				latch.countDown();
			}
		}

		@Override
		public void cancelled() {
			event.put("status", "cancelled");
			EventLogger.logEvent(event, Level.DEBUG);
			latch.countDown();
		}
	}

}
