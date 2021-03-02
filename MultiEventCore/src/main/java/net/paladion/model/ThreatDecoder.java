package net.paladion.model;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;

import kafka.utils.VerifiableProperties;

import com.paladion.racommon.model.Threat;

public class ThreatDecoder implements kafka.serializer.Decoder<Threat> {

  public ThreatDecoder(VerifiableProperties v) {
    super();
  }

  @Override
  public Threat fromBytes(byte[] arg0) {
    Threat a = null;

    try {
      Thread.currentThread().setContextClassLoader(Threat.class.getClassLoader());
      InputStream fis = null;
      fis = new ByteArrayInputStream(arg0);
      ObjectInputStream o = new ObjectInputStream(fis);
      a = (Threat) o.readObject();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return a;
  }
}
