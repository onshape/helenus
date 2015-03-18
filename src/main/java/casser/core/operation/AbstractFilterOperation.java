package casser.core.operation;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.core.dsl.Getter;

public abstract class AbstractFilterOperation<E, O extends AbstractFilterOperation<E, O>> extends AbstractObjectOperation<E, O> {

	public AbstractFilterOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public <V> O where(Getter<V> getter, String operator, V val) {
		return (O) this;
	}

	public <V> O where(Filter filter) {
		return (O) this;
	}

	public <V> O and(Getter<V> getter, String operator, V val) {
		return (O) this;
	}

	public <V> O and(Filter filter) {
		return (O) this;
	}

}
