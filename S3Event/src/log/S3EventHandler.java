package log;

import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;

/**
 * This class is for displaying AWS S3 event handling.
 * When a file uploads to s3 lambda will check its file name.
 * Since S3 can not rename file name, so using copy and delete instead.
 * 
 * @author cenxui
 * 2016/11/13
 */
public class S3EventHandler implements RequestHandler<S3Event, String> {
	
	private static final AmazonS3 s3 = new AmazonS3Client();
	
	private static final String bucketName = "awsconsole-s3-storage-lambda-log";

    @Override
    public String handleRequest(S3Event input, Context context) {
    	
        context.getLogger().log("Input: " + input);
        
        JSONObject object = new JSONObject(input.toJson());
        
        String sourceKey = object
        		.optJSONArray("Records")
        		.optJSONObject(0)
        		.optJSONObject("s3")
        		.optJSONObject("object")
        		.optString("key");
        
        if (NumberUtils.isCreatable(sourceKey.substring(0, 4)) == true) {
			return null;
		}
        
        String destinationKey = "" + (int)(Math.random()*10000) + sourceKey;
              
        CopyObjectRequest copy = new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey);
        
        s3.copyObject(copy);
        
        DeleteObjectRequest delete = new DeleteObjectRequest(bucketName, sourceKey);
        
        s3.deleteObject(delete);
        		
        return  null;
    }

}
