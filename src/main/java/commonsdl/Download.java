package commonsdl;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.kohsuke.args4j.Option;

class Download {
	@Option(name = "--file", required = true)
	private String file;
	
	@Option(name = "--charset", required = false)
	private String charset;
	
	@Option(name = "--destination", required = false)
	private String destination;
	
	public Download() {
		this.charset = "UTF-8";
		this.destination = ".";
	}
	
	public Path getFile() {
		return Paths.get(file);
	}
	
	public Path getDestination() {
		return Paths.get(destination);
	}
	
	public Charset getCharset() {
		return Charset.forName(charset);
	}
}
