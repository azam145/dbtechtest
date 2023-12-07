package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.api.model.DataHeader;
import com.db.dataplatform.techtest.client.component.Client;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    private final RestTemplate restTemplate;
    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");
    public static final UriTemplate URI_BIGDATA = new UriTemplate("ttp://localhost:8090/hadoopserver/pushbigdata");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        if(validateDataEnvelope(dataEnvelope)) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] digest = md.digest();
                String myHash = DatatypeConverter
                        .printHexBinary(digest).toLowerCase();
                dataEnvelope.setMd5CheckSum(myHash);
            } catch (NoSuchAlgorithmException algoException) {
                log.error("Unable to hash message body", algoException);
            }
            restTemplate.postForLocation(URI_PUSHDATA, dataEnvelope);
            log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        }
     }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        Map<String, String> pathVar = new HashMap<>();
        pathVar.put("blockType", blockType);
        DataEnvelope[] data = restTemplate.getForEntity(URI_GETDATA.toString(), DataEnvelope[].class, pathVar).getBody();
        List<DataEnvelope> resp = Arrays.asList(data);
        return resp;
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        Map<String, String> pathVar = new HashMap<>();
        pathVar.put("name", blockName);
        pathVar.put("newBlockType", newBlockType);
        DataHeader dh = new DataHeader(blockName, BlockTypeEnum.valueOf(blockName));
        restTemplate.patchForObject(URI_PATCHDATA.toString(),   dh, Boolean.class, pathVar);
        return true;
    }

    @Override
    public void pushBigData(DataEnvelope dataEnvelope) throws JsonProcessingException {
        if(validateDataEnvelope(dataEnvelope)) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] digest = md.digest();
                String myHash = DatatypeConverter
                        .printHexBinary(digest).toLowerCase();
                dataEnvelope.setMd5CheckSum(myHash);
            } catch (NoSuchAlgorithmException algoException) {
                log.error("Unable to hash message body", algoException);
            }
            restTemplate.postForLocation(URI_BIGDATA.toString(), dataEnvelope.toString());
            log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_BIGDATA);
        }
    }

    boolean validateDataEnvelope(DataEnvelope dataEnvelope) {
        boolean result = false;
        if(dataEnvelope == null || dataEnvelope.getDataHeader() == null) {
           log.info("Invalid Push data: dataEnvelope or header null to {}",  URI_PUSHDATA);
           return result;
        }
        switch (dataEnvelope.getDataHeader().getBlockType()) {
            case BLOCKTYPEA:
            case BLOCKTYPEB:
                result = true;
                break;
        }
        log.info("Invalid Push data: header type incorrect to {}",  URI_PUSHDATA);
        return result;
    }

}
