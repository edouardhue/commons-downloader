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
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public final class CommonsDownloader {
	
	private static final Logger LOGGER = Logger.getLogger(CommonsDownloader.class.getName());

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
			this.latch = new CountDownLatch(lines);
			Files.lines(download.getFile(), download.getCharset())
				.parallel()
				.map(s -> s.substring(0, s.lastIndexOf(',')))
				.forEach(s -> {
					this.download(s, download.getDestination());
				});
		} finally {
			try {
				latch.await();
			} catch (final InterruptedException e) {
				Thread.interrupted();
			}
			this.client.close();
		}
	}
	
	private void download(final String fileName, final Path destination) {
		try {
			final HttpGet request = new HttpGet(new URIBuilder(commons).addParameter("file", fileName).build());
			client.execute(request, new DownloaderCallback(fileName, destination));
		} catch (final URISyntaxException e) {
			LOGGER.warning(String.format("Could not build URI for %1$s because of %2$s.", fileName, e.getMessage()));
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
		
		public DownloaderCallback(final String fileName, final Path destination) {
			this.fileName = fileName;
			this.destination = destination;
		}
		
		@Override
		public void failed(final Exception e) {
			latch.countDown();
			LOGGER.warning(String.format("Donwload of %1$s failed because of %2$s.", fileName, e.getMessage()));
		}

		@Override
		public void completed(final HttpResponse response) {
			final Path destinationFile = Paths.get(destination.toString(), fileName);
			try (final BufferedOutputStream os = new BufferedOutputStream(Files.newOutputStream(destinationFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
				response.getEntity().writeTo(os);
				LOGGER.info(String.format("Downloaded %1$s.", fileName));
			} catch (final IOException e) {
				LOGGER.warning(String.format("Failed to save %1$s because of %2$s", fileName, e.getMessage()));
			} finally {
				latch.countDown();
			}
		}

		@Override
		public void cancelled() {
			latch.countDown();
		}
	}

}
