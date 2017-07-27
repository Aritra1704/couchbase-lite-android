package com.couchbase.lite;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentTest extends BaseTest {
    private static final String TAG = ConcurrentTest.class.getSimpleName();

    Document createDocumentWithTag(String tag) {
        Document doc = new Document();

        // Tag
        doc.setObject("tag", tag);

        // String
        doc.setObject("firstName", "Daniel");
        doc.setObject("lastName", "Tiger");

        // Dictionary:
        Dictionary address = new Dictionary();
        address.setObject("street", "1 Main street");
        address.setObject("city", "Mountain View");
        address.setObject("state", "CA");
        doc.setObject("address", address);

        // Array:
        Array phones = new Array();
        phones.addObject("650-123-0001");
        phones.addObject("650-123-0002");
        doc.setObject("phones", phones);

        // Date:
        doc.setObject("updated", new Date());

        return doc;
    }

    List<String> createDocs3(int nDocs, String tag) throws CouchbaseLiteException {
        List<String> docs = Collections.synchronizedList(new ArrayList<String>(nDocs));
        for (int i = 0; i < nDocs; i++) {
            Document doc = createDocumentWithTag(tag);
            db.save(doc);
            docs.add(doc.getId());
        }
        return docs;
    }

    boolean updateDocs2(List<String> docIds, int rounds, String tag) {
        int n = 0;
        for (int i = 1; i <= rounds; i++) {
            for (String docId : docIds) {
                Document doc = db.getDocument(docId);
                doc.setObject("tag", tag);

                Dictionary address = doc.getDictionary("address");
                assertNotNull(address);
                String street = String.format(Locale.ENGLISH, "%d street.", i);
                address.setObject("street", street);

                Array phones = doc.getArray("phones");
                assertNotNull(phones);
                assertEquals(2, phones.count());
                String phone = String.format(Locale.ENGLISH, "650-000-%04d", i);
                phones.setObject(0, phone);

                doc.setObject("updated", new Date());

                Log.i(TAG, "[%s] rounds: %d updating %s", tag, i, doc.getId());
                try {
                    db.save(doc);
                } catch (CouchbaseLiteException e) {
                    Log.e(TAG, "Error in Database.save()", e);
                    return false;
                }
            }
        }
        return true;
    }

    void readDocs(List<String> docIDs, int rounds) {
        for (int i = 1; i <= rounds; i++) {
            for (String docID : docIDs) {
                Document doc = db.getDocument(docID);
                assertNotNull(doc);
                assertEquals(docID, doc.getId());
            }
        }
    }

    interface VerifyBlock {
        void verify(int n, Result result);
    }

    void verifyByTagName(String tag, VerifyBlock block) throws CouchbaseLiteException {
        Expression TAG_EXPR = Expression.property("tag");
        SelectResult DOCID = SelectResult.expression(Expression.meta().getId());
        DataSource ds = DataSource.database(db);
        Query q = Query.select(DOCID).from(ds).where(TAG_EXPR.equalTo(tag));
        Log.e(TAG, "query - > %s", q.explain());
        ResultSet rs = q.run();
        Result result;
        int n = 0;
        while ((result = rs.next()) != null) {
            block.verify(++n, result);
        }

    }

    void verifyByTagName(String tag, int nRows) throws CouchbaseLiteException {
        final AtomicInteger count = new AtomicInteger(0);
        verifyByTagName(tag, new VerifyBlock() {
            @Override
            public void verify(int n, Result result) {
                count.incrementAndGet();
            }
        });
        assertEquals(nRows, count.intValue());
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentCreate() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 2000;

        final CountDownLatch latch1 = new CountDownLatch(1);
        final String tag1 = "Create1";
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    createDocs3(kNDocs, tag1);
                } catch (CouchbaseLiteException e) {
                    fail();
                }
                latch1.countDown();
            }
        }).start();

        final CountDownLatch latch2 = new CountDownLatch(1);
        final String tag2 = "Create2";
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    createDocs3(kNDocs, tag2);
                } catch (CouchbaseLiteException e) {
                    fail();
                }
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
        assertTrue(latch2.await(60, TimeUnit.SECONDS));

        verifyByTagName(tag1, kNDocs);
        verifyByTagName(tag2, kNDocs);
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentUpdate() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 20;
        final int kNRounds = 100;

        // createDocs2 returns synchronized List.
        final List<String> docs = createDocs3(kNDocs, "Create");
        assertEquals(kNDocs, docs.size());

        final CountDownLatch latch1 = new CountDownLatch(1);
        final String tag1 = "Update1";
        new Thread(new Runnable() {
            @Override
            public void run() {
                assertTrue(updateDocs2(docs, kNRounds, tag1));
                latch1.countDown();
            }
        }).start();

        final CountDownLatch latch2 = new CountDownLatch(1);
        final String tag2 = "Update2";
        new Thread(new Runnable() {
            @Override
            public void run() {
                assertTrue(updateDocs2(docs, kNRounds, tag2));
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(180, TimeUnit.SECONDS));
        assertTrue(latch2.await(60, TimeUnit.SECONDS));


        final AtomicInteger count = new AtomicInteger(0);

        verifyByTagName(tag1, new VerifyBlock() {
            @Override
            public void verify(int n, Result result) {
                count.incrementAndGet();
            }
        });
        verifyByTagName(tag2, new VerifyBlock() {
            @Override
            public void verify(int n, Result result) {
                count.incrementAndGet();
            }
        });

        assertEquals(kNDocs, count.intValue());
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentRead() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 20;
        final int kNRounds = 100;

        // createDocs2 returns synchronized List.
        final List<String> docIDs = createDocs3(kNDocs, "Create");
        assertEquals(kNDocs, docIDs.size());


        final CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                readDocs(docIDs, kNRounds);
                latch1.countDown();
            }
        }).start();

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                readDocs(docIDs, kNRounds);
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
        assertTrue(latch2.await(60, TimeUnit.SECONDS));
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentReadNUpdate() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 20;
        final int kNRounds = 100;

        // createDocs2 returns synchronized List.
        final List<String> docIDs = createDocs3(kNDocs, "Create");
        assertEquals(kNDocs, docIDs.size());

        // Read:
        final CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                readDocs(docIDs, kNRounds);
                latch1.countDown();
            }
        }).start();

        // Update:
        final CountDownLatch latch2 = new CountDownLatch(1);
        final String tag = "Update";
        new Thread(new Runnable() {
            @Override
            public void run() {
                assertTrue(updateDocs2(docIDs, kNRounds, tag));
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
        assertTrue(latch2.await(60, TimeUnit.SECONDS));

        verifyByTagName(tag, kNDocs);
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentDelete() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 2000;

        // createDocs2 returns synchronized List.
        final List<String> docIDs = createDocs3(kNDocs, "Create");
        assertEquals(kNDocs, docIDs.size());

        final CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (String docID : docIDs) {
                    try {
                        Document doc = db.getDocument(docID);
                        db.delete(doc);
                    } catch (CouchbaseLiteException e) {
                        Log.e(TAG, "Error in Database.delete(Document)", e);
                        fail();
                    }
                }
                latch1.countDown();
            }
        }).start();

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (String docID : docIDs) {
                    try {
                        db.delete(db.getDocument(docID));
                    } catch (CouchbaseLiteException e) {
                        Log.e(TAG, "Error in Database.delete(Document)", e);
                        fail();
                    }
                }
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
        assertTrue(latch2.await(60, TimeUnit.SECONDS));

        assertEquals(0, db.getCount());
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentPurge() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 2000;

        // createDocs2 returns synchronized List.
        final List<String> docIDs = createDocs3(kNDocs, "Create");
        assertEquals(kNDocs, docIDs.size());

        final CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (String docID : docIDs) {
                    try {
                        Document doc = db.getDocument(docID);
                        db.purge(doc);
                    } catch (CouchbaseLiteException e) {
                        assertEquals(404, e.getCode());
                    }
                }
                latch1.countDown();
            }
        }).start();

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (String docID : docIDs) {
                    try {
                        db.purge(db.getDocument(docID));
                    } catch (CouchbaseLiteException e) {
                        assertEquals(404, e.getCode());
                    }
                }
                latch2.countDown();
            }
        }).start();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
        //assertTrue(latch2.await(60, TimeUnit.SECONDS));

        assertEquals(0, db.getCount());
    }

    // NOTE: Cause crash now!!!!
    @Test
    public void testConcurrentCreateNCloseDB() throws InterruptedException, CouchbaseLiteException {
        final int kNDocs = 2000;

        final CountDownLatch latch1 = new CountDownLatch(1);
        final String tag1 = "Create1";
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    createDocs3(kNDocs, tag1);
                } catch (CouchbaseLiteException e) {
                    fail();
                }
                latch1.countDown();
            }
        }).start();

        db.close();

        assertTrue(latch1.await(60, TimeUnit.SECONDS));
    }

    @Test
    public void testBlockDatabaseChange() throws InterruptedException, CouchbaseLiteException {

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        db.addChangeListener(new DatabaseChangeListener() {
            @Override
            public void changed(DatabaseChange change) {
                try {
                    assertTrue(latch1.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    fail();
                }
                latch2.countDown();
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    db.save(new Document("doc1"));
                } catch (CouchbaseLiteException e) {
                    fail();
                }
                latch1.countDown();
            }
        }).start();

        assertTrue(latch2.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testBlockDocumentChange() throws InterruptedException, CouchbaseLiteException {

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        db.addChangeListener("doc1", new DocumentChangeListener() {
            @Override
            public void changed(DocumentChange change) {
                try {
                    assertTrue(latch1.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    fail();
                }
                latch2.countDown();
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    db.save(new Document("doc1"));
                } catch (CouchbaseLiteException e) {
                    fail();
                }
                latch1.countDown();
            }
        }).start();

        assertTrue(latch2.await(10, TimeUnit.SECONDS));
    }
}
