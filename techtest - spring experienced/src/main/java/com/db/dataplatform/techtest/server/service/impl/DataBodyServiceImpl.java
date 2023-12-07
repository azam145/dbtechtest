package com.db.dataplatform.techtest.server.service.impl;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.repository.DataStoreRepository;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DataBodyServiceImpl implements DataBodyService {

    private final DataStoreRepository dataStoreRepository;

    @Override
    public void saveDataBody(DataBodyEntity dataBody) {
        dataStoreRepository.save(dataBody);
    }

    @Override
    public List<DataBodyEntity> getDataByBlockType(BlockTypeEnum blockType) {
        return dataStoreRepository
                .findAll()
                .stream()
                .filter(entry->entry.getDataHeaderEntity().getBlocktype().toString().equals(blockType.toString()))
                .collect(Collectors.toList());
    }


    /**
     *  This assumes that there is only one entry per name.
     * @param blockName
     * @return
     */
    @Override
    public Optional<DataBodyEntity> getDataByBlockName(String blockName) {
        return dataStoreRepository
                .findAll()
                .stream()
                .filter(entry->entry.getDataHeaderEntity().getName().equals(blockName))
                .findAny();
    }
}
