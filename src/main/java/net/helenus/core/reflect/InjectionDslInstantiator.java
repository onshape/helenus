/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core.reflect;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.datastax.driver.core.Metadata;
import com.sun.tools.classfile.TypeAnnotation;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.matcher.ElementMatchers;
import net.helenus.config.HelenusSettings;
import net.helenus.core.DslInstantiator;
import net.helenus.core.Helenus;
import net.helenus.mapping.annotation.Enlisted;

import static net.bytebuddy.matcher.ElementMatchers.named;

public enum InjectionDslInstantiator implements DslInstantiator {

    INSTANCE;

    @Override
    @SuppressWarnings("unchecked")
    public <E> E instantiate(Class<E> iface, ClassLoader classLoader, Optional<HelenusPropertyNode> parent,
            Metadata metadata) {

        HelenusSettings settings = Helenus.settings();
        Enlisted enlisted = iface.getDeclaredAnnotation(Enlisted.class);

        List<Method> methods = new ArrayList<Method>();
        methods.addAll(Arrays.asList(iface.getDeclaredMethods()));
        for (Class<?> c : iface.getInterfaces()) {
            methods.addAll(Arrays.asList(c.getDeclaredMethods()));
        }
        methods = methods.stream().filter(method -> settings.getGetterMethodDetector().apply(method))
                .collect(Collectors.toList());


        try {
            DynamicType.Builder<?> dynamicType = new ByteBuddy()
                    .with(new NamingStrategy.AbstractBase() {
                        @Override protected String name(TypeDescription superClass) {
                            return iface.getPackage().getName() + ".Enlisted" + iface.getSimpleName();
                        }
                    })
                    .subclass(enlisted.value(), ConstructorStrategy.Default.IMITATE_SUPER_CLASS)
                    .implement(MapExportable.class)
                    .implement(iface);

            Arrays.asList(iface.getInterfaces()).forEach((i) -> dynamicType.implement(i) );
            methods.forEach(m -> dynamicType.defineField(m.getName(), m.getReturnType(), Visibility.PRIVATE));
            methods.forEach(m -> {
                dynamicType.defineMethod(m.getName(), m.getReturnType(), Visibility.PUBLIC);
                dynamicType.
                dynamicType.method(named(m.getName())).intercept(FieldProxy.FieldGetter())
                if (m.getReturnType() == Boolean.class) {

                }
            });

        }
        catch (IllegalAccessError e) {
        }
/*
                    .method(ElementMatchers.named("toString"))
                    .intercept(FixedValue.value("Hello World!"))
                    .make();
                    .make();
                            .load(getClass().getClassLoader())
                .getLoaded();

        return dynamicType.newInstance();
        */

    }
}
