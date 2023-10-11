package com.db.dataplatform.techtest.server.component.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

	private final DataBodyService dataBodyServiceImpl;
	private final ModelMapper modelMapper;
	private final RestTemplate restTemplate;

	private static final String BIG_DATA_URI = "http://localhost:8090/hadoopserver/pushbigdata";

	/**
	 * Using custom Threadpool for calling the Hadoop Service to ensure you dont
	 * need to wait on default ForkJoinThreadpool
	 */
	private final ExecutorService service = Executors.newFixedThreadPool(5);

	/**
	 * @param envelope dataenvelop received for persistence
	 * @return true if there is a match with the client provided checksum.
	 */
	@Override
	public boolean saveDataEnvelope(DataEnvelope envelope) {
		// Calling the Bigdata Persistence before actual persistence and then return
		// once both the outputs are ready
		try {
			if(!validateChecksum(envelope)) {
				return false;
			}
			CompletableFuture<ResponseEntity<HttpStatus>> completableFuture = CompletableFuture.supplyAsync(() -> {
				return restTemplate.postForEntity(BIG_DATA_URI,
						envelope.getDataBody().getDataBody(), HttpStatus.class);
			}, service);
			// Save to persistence.
			persist(envelope);
			log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());

			ResponseEntity<HttpStatus> response = completableFuture.get();
			return response.getStatusCode() == HttpStatus.OK;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return false;
		}
	}

	private boolean validateChecksum(DataEnvelope dataEnvelope) throws NoSuchAlgorithmException {
		/*
		 * Recomputing the digest to compute the MD5 hash again and comparing with one in body
		 */
		MessageDigest digest = MessageDigest.getInstance("MD5");
		byte[] computestDigest = digest.digest(dataEnvelope.getDataBody().getDataBody().getBytes());

		//Applying to lowercase as MD5 can be passed in Upper as well based on generator
		return Hex.encodeHexString(computestDigest).equals(dataEnvelope.getChecksum().toLowerCase());
	}

	private void persist(DataEnvelope envelope) {
		log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
		DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);
		DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
		dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
		saveData(dataBodyEntity);
	}

	private void saveData(DataBodyEntity dataBodyEntity) {
		dataBodyServiceImpl.saveDataBody(dataBodyEntity);
	}

	@Override
	public DataEnvelope updateDataEnvelopeBlockType(BlockTypeEnum blockType, String name) {
		Optional<DataBodyEntity> body = dataBodyServiceImpl.getDataByBlockName(name);
		if (body.isPresent()) {
			body.get().getDataHeaderEntity().setBlocktype(blockType);
			dataBodyServiceImpl.saveDataBody(body.get());
			return convertEntity(body.get());
		} else {
			return null;
		}
	}

	@Override
	public List<DataEnvelope> getDataEnvelopByBlockType(BlockTypeEnum blocktype) {
		List<DataBodyEntity> listOfDataBodyEntity = dataBodyServiceImpl.getDataByBlockType(blocktype);
		return listOfDataBodyEntity.parallelStream().map(this::convertEntity)
				.collect(Collectors.toList());
	}

	private DataEnvelope convertEntity(DataBodyEntity body) {
		DataHeader dataHeader = new DataHeader(body.getDataHeaderEntity().getName(),
				body.getDataHeaderEntity().getBlocktype());
		DataBody dataBody = new DataBody(body.getDataBody());
		
		return new DataEnvelope(dataHeader, dataBody, "");
	}

}
