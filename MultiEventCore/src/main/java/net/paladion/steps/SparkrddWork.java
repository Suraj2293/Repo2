package net.paladion.steps;


public interface SparkrddWork<A, Z> {

  public Z transform(A rdd);

}
