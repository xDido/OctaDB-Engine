package Exceptions;

import java.io.IOException;

public class DBAppException extends Exception{
    public DBAppException(String message) {
        super(message);
    }

	public DBAppException(IOException e) {
		System.out.print(e.getMessage());
	}

	public DBAppException(Exception e) {
		System.out.print(e.getMessage());
	}

}
