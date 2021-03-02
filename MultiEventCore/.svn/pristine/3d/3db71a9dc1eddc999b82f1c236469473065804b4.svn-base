package net.paladion.steps;

import java.io.Serializable;

public abstract class SparkStep<A, Z> implements SparkrddWork<A, Z>, Serializable {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  @Override
  public Z transform(A threadRdd) {
    // TODO Auto-generated method stub
    return customTransform(threadRdd);
  }

  public abstract Z customTransform(A rdd);

}
