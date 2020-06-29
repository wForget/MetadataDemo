package cn.wang.demo.metadata.elasticsearch;

import cn.wang.demo.metadata.conf.ConfUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MetaDataMain {

    public static void main(String[] args) throws IOException {
        String cluster = ConfUtils.getString("elasticsearch.cluster");
        List<HttpHost> hosts = Arrays.stream(cluster.split(","))
                .map(item -> new HttpHost(item.split(":")[0], Integer.valueOf(item.split(":")[1])))
                .collect(Collectors.toList());
        try (RestClient restClient = RestClient
                .builder(hosts.toArray(new HttpHost[hosts.size()]))
                .build()) {
            Request request = new Request("GET", "_stats");
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println(responseBody);

            System.out.println("----------indices---------");
            JSONObject jsonObject = JSON.parseObject(responseBody);
            JSONObject indices = jsonObject.getJSONObject("indices");
            indices.entrySet().forEach(entry -> {
                String indexName = entry.getKey();
                JSONObject indexObject = (JSONObject) entry.getValue();
                System.out.println(indexName + "\t" + indexObject.toString());

            });
        } finally {

        }
    }

}
