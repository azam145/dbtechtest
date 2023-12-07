package com.db.dataplatform.techtest.api.controller;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.util.UriTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

@RunWith(MockitoJUnitRunner.class)
public class ServerControllerComponentTest {

	public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
	public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
	public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

	@Mock
	private Server serverMock;

	private DataEnvelope testDataEnvelope;
	private ObjectMapper objectMapper;
	private MockMvc mockMvc;
	private ServerController serverController;

	@Before
	public void setUp() throws HadoopClientException, NoSuchAlgorithmException, IOException {
		serverController = new ServerController(serverMock);
		mockMvc = standaloneSetup(serverController).build();
		objectMapper = Jackson2ObjectMapperBuilder
				.json()
				.build();

	}

	@Test
	public void testPushDataPostCallWorksAsExpected() throws Exception {

		when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);

		testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();
		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
				.content(testDataEnvelopeJson)
				.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isTrue();
	}

	@Test
	public void testPushDataPostCallFailCheckSum() throws Exception {

		testDataEnvelope = TestDataHelper.createTestDataEnvIncorrectHash();
		String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

		MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
						.content(testDataEnvelopeJson)
						.contentType(MediaType.APPLICATION_JSON_VALUE))
				.andExpect(status().isOk())
				.andReturn();

		boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
		assertThat(checksumPass).isFalse();
	}

	@Test
	public void testGetDataSucess() throws Exception {

		DataHeaderEntity dh = new DataHeaderEntity();
		DataBodyEntity db = new DataBodyEntity();
		db.setDataHeaderEntity(dh);
		dh.setBlocktype(BlockTypeEnum.BLOCKTYPEA);
		List<DataBodyEntity> list = Arrays.asList(db);

		when(serverMock.getDataByBlockType(BlockTypeEnum.BLOCKTYPEA)).thenReturn(list);

		MvcResult mvcResult = mockMvc.perform(
				get(String.valueOf(URI_GETDATA), BlockTypeEnum.BLOCKTYPEA))
				.andExpect(status().isOk())
				.andReturn();


		String json = mvcResult.getResponse().getContentAsString();
		List<DataBodyEntity> rslt = new ObjectMapper().readValue(json, new TypeReference<List<DataBodyEntity>>() {});
		Assertions.assertEquals(rslt.size(), 1);
		Assertions.assertTrue(rslt.get(0).getDataHeaderEntity().getBlocktype().toString().equals(BlockTypeEnum.BLOCKTYPEA.toString()));
	}

	@Test
	public void testGetDataFail() throws Exception {

		List<DataBodyEntity> list = new ArrayList<>();
		when(serverMock.getDataByBlockType(BlockTypeEnum.BLOCKTYPEA)).thenReturn(list);
		MvcResult mvcResult = mockMvc.perform(
						get(String.valueOf(URI_GETDATA), BlockTypeEnum.BLOCKTYPEA))
				.andExpect(status().isNotFound())
				.andReturn();

		String json = mvcResult.getResponse().getContentAsString();
		Assertions.assertTrue(json.equals(""));
	}


	@Test
	public void testpatchHeaderSuccess() throws Exception {
		DataBodyEntity db = new DataBodyEntity();
		DataHeaderEntity dh = new DataHeaderEntity();
		dh.setName("test");
		dh.setBlocktype(BlockTypeEnum.BLOCKTYPEA);
		db.setDataHeaderEntity(dh);
		db.setDataBody("test12333");
		String headerName = "test";
		when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);
		when(serverMock.getDataByBlockByName(headerName)).thenReturn(Optional.of(db));

		MvcResult mvcResult = mockMvc.perform(
						put(String.valueOf(URI_PATCHDATA), headerName, BlockTypeEnum.BLOCKTYPEA.toString()))
				.andExpect(status().isOk())
				.andReturn();
		Assertions.assertTrue(true);
	}

	public void testpatchHeaderFail() {
		/**
		 *  TODO
		 */
	}

	@Test
	public void testHadoopCallPass() {
		// TODO
	}

	@Test
	public void testHadoopCallFail() {
		// TODO
	}


}
