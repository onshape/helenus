/*
 *      Copyright (C) 2015 Noorq, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.noorq.casser.test.integration.core.udtcollection;

import static com.noorq.casser.core.Query.eq;
import static com.noorq.casser.core.Query.getIdx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class UDTCollectionTest extends AbstractEmbeddedCassandraTest {

	static Book book = Casser.dsl(Book.class);
	
	static CasserSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Casser.init(getSession()).showCql().add(book).autoCreateDrop().get();
	}
	
	@Test
	public void test() {
		System.out.println(book);	
	}
	
	@Test
	public void testSetCRUID() {
		
		int id = 555;
		
		// CREATE
		
		Set<Author> reviewers = new HashSet<Author>();
		reviewers.add(new AuthorImpl("Alex", "San Jose"));
		reviewers.add(new AuthorImpl("Bob", "San Francisco"));
		
		session.insert()
		.value(book::id, id)
		.value(book::reviewers, reviewers)
		.sync();
		
		// READ
		
		Book actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		assertEqualSets(reviewers, actual.reviewers());
		
		// UPDATE
		
		Set<Author> expected = new HashSet<Author>();
		expected.add(new AuthorImpl("Craig", "Los Altos"));
		
		session.update().set(book::reviewers, expected).where(book::id, eq(id)).sync();
		
		Set<Author> actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualSets(expected, actualSet);
		
		// add operation
		
		expected.add(new AuthorImpl("Add", "AddCity"));
		session.update().add(book::reviewers, new AuthorImpl("Add", "AddCity"))
			.where(book::id, eq(id)).sync();
		
		actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualSets(expected, actualSet);
		
		// addAll operation
		expected.addAll(reviewers);
		session.update().addAll(book::reviewers, reviewers).where(book::id, eq(id)).sync();
		
		actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualSets(expected, actualSet);
		
		// DELETE
		
		// remove single value
		
		Author a = expected.stream().filter(p -> p.name().equals("Add")).findFirst().get();
		expected.remove(a);
		
		session.update().remove(book::reviewers, a).where(book::id, eq(id)).sync();
		
		actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualSets(expected, actualSet);
		
		// remove values
		
		expected.remove(expected.stream().filter(p -> p.name().equals("Alex")).findFirst().get());
		expected.remove(expected.stream().filter(p -> p.name().equals("Bob")).findFirst().get());
		session.update().removeAll(book::reviewers, reviewers).where(book::id, eq(id)).sync();
		
		actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualSets(expected, actualSet);
		
		// remove full list
		
		session.update().set(book::reviewers, null).where(book::id, eq(id)).sync();
		
		actualSet = session.select(book::reviewers).where(book::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualSet);
		
		// remove object
		
		session.delete().where(book::id, eq(id)).sync();
		Long cnt = session.count().where(book::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
		
	}
	
	private void assertEqualSets(Set<Author> expected, Set<Author> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		
		for (Author e : expected) {
			Author a = actual.stream().filter(p -> p.name().equals(e.name())).findFirst().get();
			Assert.assertEquals(e.city(), a.city());
		}
		
	}
	
	
	@Test
	public void testListCRUID() {
		
		int id = 777;
		
		List<Author> authors = new ArrayList<Author>();
		authors.add(new AuthorImpl("Alex", "San Jose"));
		authors.add(new AuthorImpl("Bob", "San Francisco"));
		
		// CREATE
		
		session.insert()
		.value(book::id, id)
		.value(book::authors, authors)
		.sync();
		
		// READ
		
		// read full object
		
		Book actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		assertEqualLists(authors, actual.authors());
		Assert.assertNull(actual.reviewers());
		Assert.assertNull(actual.contents());
		
		// read full list
		
		List<Author> actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(authors, actualList);
		
		// read single value by index
		
		String cql = session.select(getIdx(book::authors, 1))
				.where(book::id, eq(id)).cql();

		System.out.println("Still not supporting cql = " + cql);
		
		// UPDATE
		
		List<Author> expected = new ArrayList<Author>();
		expected.add(new AuthorImpl("Unknown", "City 17"));
		
		session.update().set(book::authors, expected).where(book::id, eq(id)).sync();
		
		actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		assertEqualLists(expected, actual.authors());
		
		// INSERT
		
		// prepend operation
		
		expected.add(0, new AuthorImpl("Prepend", "PrependCity"));
		session.update().prepend(book::authors, new AuthorImpl("Prepend", "PrependCity")).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// append operation
		
		expected.add(new AuthorImpl("Append", "AppendCity"));
		session.update().append(book::authors, new AuthorImpl("Append", "AppendCity")).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// prependAll operation
		expected.addAll(0, authors);
		session.update().prependAll(book::authors, authors).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// appendAll operation
		expected.addAll(authors);
		session.update().appendAll(book::authors, authors).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// set by Index
		
		Author inserted = new AuthorImpl("Insert", "InsertCity");
		expected.set(5, inserted);
		session.update().setIdx(book::authors, 5, inserted).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// DELETE
		
		// remove single value
		
		expected.remove(inserted);
		session.update().discard(book::authors, inserted).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// remove values
		
		expected.removeAll(authors);
		session.update().discardAll(book::authors, authors).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualLists(expected, actualList);
		
		// remove full list
		
		session.update().set(book::authors, null).where(book::id, eq(id)).sync();
		
		actualList = session.select(book::authors).where(book::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualList);
		
		// remove object
		
		session.delete().where(book::id, eq(id)).sync();
		Long cnt = session.count().where(book::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
	private void assertEqualLists(List<Author> expected, List<Author> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		
		int size = expected.size();
		
		for (int i = 0; i != size; ++i) {
			Author e = expected.get(i);
			Author a = actual.get(i);
			Assert.assertEquals(e.name(), a.name());
			Assert.assertEquals(e.city(), a.city());
		}
		
	}
	
	private static final class AuthorImpl implements Author {

		String name;
		String city;
		
		AuthorImpl(String name, String city) {
			this.name = name;
			this.city = city;
		}
		
		@Override
		public String name() {
			return name;
		}

		@Override
		public String city() {
			return city;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AuthorImpl other = (AuthorImpl) obj;
			if (city == null) {
				if (other.city != null)
					return false;
			} else if (!city.equals(other.city))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "AuthorImpl [name=" + name + ", city=" + city + "]";
		}
		
	}

	private static final class SectionImpl implements Section {

		String title;
		int page;
		
		SectionImpl(String title, int page) {
			this.title = title;
			this.page = page;
		}
		
		@Override
		public String title() {
			return title;
		}

		@Override
		public int page() {
			return page;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + page;
			result = prime * result + ((title == null) ? 0 : title.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SectionImpl other = (SectionImpl) obj;
			if (page != other.page)
				return false;
			if (title == null) {
				if (other.title != null)
					return false;
			} else if (!title.equals(other.title))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "SectionImpl [title=" + title + ", page=" + page + "]";
		}
		
	}
	
	
}


