package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope);

        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());
        return ResponseEntity.ok(checksumPass);
    }

   @GetMapping(value = "/data/{blockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataBodyEntity>> getData(@PathVariable BlockTypeEnum blockType) {

        log.info("Get request received for block type: {}", blockType);
        List<DataBodyEntity> data = server.getDataByBlockType(blockType);
        if(data.size() > 0) {
            return ResponseEntity.ok(data);
        }
        return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
    }

    @PutMapping(value = "/update/{name}/{newBlockType}")
    public ResponseEntity<Boolean> patchHeaderBlockType(@PathVariable String name, @PathVariable String newBlockType) throws NoSuchAlgorithmException, IOException {
        boolean resp = false;
        log.info("Get request received for block name: {} Get request received for block type", name, newBlockType);
        Optional<DataBodyEntity> data = server.getDataByBlockByName(name);
        if(data.isPresent()) {
            DataHeader dh = new DataHeader(name, BlockTypeEnum.valueOf(newBlockType));
            String dbStr = data.get().getDataBody();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(dbStr.getBytes());
            byte[] digest = md.digest();
            String myHash = DatatypeConverter
                    .printHexBinary(digest).toUpperCase();
            DataBody db = new DataBody(dbStr);
            DataEnvelope de = new DataEnvelope(dh, db,  myHash);
            server.saveDataEnvelope(de);
            return ResponseEntity.ok(resp);
        }
        return new ResponseEntity<>(resp, HttpStatus.NOT_FOUND);
    }

}
