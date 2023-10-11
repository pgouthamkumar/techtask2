package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;

import java.util.List;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope);
    DataEnvelope updateDataEnvelopeBlockType(BlockTypeEnum blockType, String name);
	List<DataEnvelope> getDataEnvelopByBlockType(BlockTypeEnum blocktype);
}
