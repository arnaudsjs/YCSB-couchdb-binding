package couchdbBinding.java;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.ektorp.AttachmentInputStream;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbInfo;
import org.ektorp.DesignDocInfo;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.DocumentOperationResult;
import org.ektorp.Options;
import org.ektorp.Page;
import org.ektorp.PageRequest;
import org.ektorp.PurgeResult;
import org.ektorp.ReplicationStatus;
import org.ektorp.Revision;
import org.ektorp.StreamingChangesResult;
import org.ektorp.StreamingViewResult;
import org.ektorp.UpdateConflictException;
import org.ektorp.UpdateHandlerRequest;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

/*
 * This CouchDbConnector load balances the request to
 * the different nodes in the couchdb cluster.
 * 
 * Note: Only the create, get, update and delete methods are implemented. 
 * 
 * ***********************************************************************
 * 
 * Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Administrative Contact: dnet-project-office@cs.kuleuven.be
 * Technical Contact: arnaud.schoonjans@student.kuleuven.be
 */
public class LoadBalancedConnector implements CouchDbConnector{

	private final List<CouchDbConnector> connectors;
	private int nextConnector;
	
	public LoadBalancedConnector(List<URL> urlsOfNodesInCluster, String databaseName){
		if(urlsOfNodesInCluster == null)
			throw new IllegalArgumentException("urlsOfNodesInClusterIsNull");
		if(urlsOfNodesInCluster.isEmpty())
			throw new IllegalArgumentException("At least one node required");
		this.connectors = this.createConnectors(urlsOfNodesInCluster, databaseName);
		this.nextConnector = 0;
	}
	
	private List<CouchDbConnector> createConnectors(List<URL> urlsForConnectors, String databaseName){
		List<CouchDbConnector> result = new ArrayList<CouchDbConnector>();
		for(URL url : urlsForConnectors){
			HttpClient httpClient = new StdHttpClient.Builder().url(url).build();
			CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
			// 2nd paramter true => Create database if not exists
			CouchDbConnector dbConnector = dbInstance.createConnector(databaseName, true);
			result.add(dbConnector);
		}
		return result;
	}
	
	private void updateNextConnector(){
		this.nextConnector = (this.nextConnector+1) % this.connectors.size();
	}
	
	private CouchDbConnector getConnector(){
		CouchDbConnector result = this.connectors.get(this.nextConnector);
		this.updateNextConnector();
		return result;
	}

	private CouchDbConnector getConnectorForMutationOperations(){
		return this.connectors.get(0);
	}
	
	@Override
	public void create(String id, Object o) {
		boolean failed = true;
		for(int i=0; i<this.connectors.size() && failed; i++){
			try{
				this.getConnectorForMutationOperations().create(id, o);
				failed = false;
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		if(failed)
			throw new NoNodeReacheableException();
	}

	@Override
	public void create(Object o) {
		boolean failed = true;
		for(int i=0; i<this.connectors.size() && failed; i++){
			try{
				this.getConnectorForMutationOperations().create(o);
				failed = false;
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		if(failed)
			throw new NoNodeReacheableException();
	}

	@Override
	public void update(Object o) {
		boolean failed = true;
		for(int i=0; i<this.connectors.size() && failed; i++){
			try{
				this.getConnectorForMutationOperations().update(o);
				failed = false;
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		if(failed)
			throw new NoNodeReacheableException();
	}

	@Override
	public String delete(Object o) {
		for(int i=0; i<this.connectors.size(); i++){
			try{
				return this.getConnectorForMutationOperations().delete(o);
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		throw new NoNodeReacheableException();
	}

	@Override
	public String delete(String id, String revision) {
		for(int i=0; i<this.connectors.size(); i++){
			try{
				return this.getConnectorForMutationOperations().delete(id, revision);
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		throw new NoNodeReacheableException();
	}

	@Override
	public String copy(String sourceDocId, String targetDocId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String copy(String sourceDocId, String targetDocId,
			String targetRevision) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public PurgeResult purge(Map<String, List<String>> revisionsToPurge) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public <T> T get(Class<T> c, String id) {
		for(int i=0; i<this.connectors.size(); i++){
			try{
				return this.getConnector().get(c, id);
			} catch(DocumentNotFoundException exc){
				throw exc;
			} catch(Exception exc){}
		}
		throw new NoNodeReacheableException();
	}

	@Override
	public <T> T get(Class<T> c, String id, Options options) {
		for(int i=0; i<this.connectors.size(); i++){
			try{
				return this.getConnector().get(c, id, options);
			} catch(DocumentNotFoundException exc){
				throw exc;
			} catch(Exception exc){}
		}
		throw new NoNodeReacheableException();
	}

	@Override
	public <T> T find(Class<T> c, String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public <T> T find(Class<T> c, String id, Options options) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	@Deprecated
	public <T> T get(Class<T> c, String id, String rev) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	@Deprecated
	public <T> T getWithConflicts(Class<T> c, String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public boolean contains(String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public InputStream getAsStream(String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	@Deprecated
	public InputStream getAsStream(String id, String rev) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public InputStream getAsStream(String id, Options options) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<Revision> getRevisions(String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String getCurrentRevision(String id) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public AttachmentInputStream getAttachment(String id, String attachmentId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public AttachmentInputStream getAttachment(String id, String attachmentId,
			String revision) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String createAttachment(String docId, AttachmentInputStream data) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String createAttachment(String docId, String revision,
			AttachmentInputStream data) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String deleteAttachment(String docId, String revision,
			String attachmentId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<String> getAllDocIds() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public <T> List<T> queryView(ViewQuery query, Class<T> type) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public <T> Page<T> queryForPage(ViewQuery query, PageRequest pr,
			Class<T> type) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ViewResult queryView(ViewQuery query) {
		for(int i=0; i<this.connectors.size(); i++){
			try{
				return this.getConnector().queryView(query);
			} catch(Exception exc){}
		}
		throw new NoNodeReacheableException();
	}

	@Override
	public StreamingViewResult queryForStreamingView(ViewQuery query) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public InputStream queryForStream(ViewQuery query) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void createDatabaseIfNotExists() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String getDatabaseName() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String path() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public HttpClient getConnection() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public DbInfo getDbInfo() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public DesignDocInfo getDesignDocInfo(String designDocId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void compact() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void compactViews(String designDocumentId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void cleanupViews() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public int getRevisionLimit() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void setRevisionLimit(int limit) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ReplicationStatus replicateFrom(String source) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ReplicationStatus replicateFrom(String source,
			Collection<String> docIds) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ReplicationStatus replicateTo(String target) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ReplicationStatus replicateTo(String target,
			Collection<String> docIds) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void addToBulkBuffer(Object o) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentOperationResult> flushBulkBuffer() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void clearBulkBuffer() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentOperationResult> executeBulk(InputStream inputStream) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentOperationResult> executeAllOrNothing(
			InputStream inputStream) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentOperationResult> executeBulk(Collection<?> objects) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentOperationResult> executeAllOrNothing(
			Collection<?> objects) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<DocumentChange> changes(ChangesCommand cmd) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public StreamingChangesResult changesAsStream(ChangesCommand cmd) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ChangesFeed changesFeed(ChangesCommand cmd) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String callUpdateHandler(String designDocID, String function,
			String docId) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String callUpdateHandler(String designDocID, String function,
			String docId, Map<String, String> params) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public <T> T callUpdateHandler(UpdateHandlerRequest req, Class<T> c) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public String callUpdateHandler(UpdateHandlerRequest req) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void ensureFullCommit() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void updateMultipart(String id, InputStream stream, String boundary,
			long length, Options options) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public void update(String id, InputStream document, long length,
			Options options) {
		boolean failed = true;
		for(int i=0; i<this.connectors.size() && failed; i++){
			try{
				this.getConnectorForMutationOperations().update(id, document, length, options); 
				failed = false;
			} catch(UpdateConflictException exc){
				throw exc;
			} catch(Exception exc){}
		}
		if(failed)
			throw new NoNodeReacheableException();
	}
}
